import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    expr,
    concat_ws,
    lag,
    to_date,
    lit,
    substring,
    coalesce,
)
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.window import Window
from src.preprocesamiento import (
    cargar_datos_fuente_a,
    crear_variable_objetivo,
    procesar_fuente_c,
)
from datetime import date
import os
import shutil
import tempfile


# --- Fixture de Spark (se inicia una vez por sesión de test) ---
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("Pytest-ETL-Preprocesamiento")
        .master("local[2]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


# --- Tests para crear_variable_objetivo (Fuente A) ---
def test_crear_variable_objetivo(spark):
    """
    Test 1.1: Verificar que `accidente_grave` se crea correctamente (Fase 1).
    """
    mock_data_a = [(1, 4), (2, 3), (3, 1), (4, 2), (5, 7), (6, 14), (7, 77), (8, None)]
    mock_df_a = spark.createDataFrame(mock_data_a, ["num_expediente", "cod_lesividad"])
    df_resultado = crear_variable_objetivo(mock_df_a)
    resultados = {
        row.num_expediente: row.accidente_grave for row in df_resultado.collect()
    }

    assert resultados[1] == 1  # Fallecido (04)
    assert resultados[2] == 1  # Grave (03)
    assert resultados[3] == 0  # Leve (01)
    assert resultados[4] == 0  # Leve (02)
    assert resultados[5] == 0  # Leve (07)
    assert resultados[6] == 0  # Sin asistencia (14)
    assert resultados[7] == 0  # Desconocido (77)
    assert resultados[8] == 0  # Nulo


# --- Fixtures para cargar_datos_fuente_a (Fuente A) ---
@pytest.fixture(scope="session")
def mock_accidentes_path(spark):
    """Crea archivos CSV mock para probar la unión de Fuente A."""
    ruta_temp = tempfile.mkdtemp()
    ruta_accidentes = os.path.join(ruta_temp, "accidentes")
    os.makedirs(ruta_accidentes)

    # Archivo 1 (2019) usa 'tipo_lesividad' y '.' como separador de miles
    csv_2019_header = "num_expediente;fecha;hora;localizacion;numero;cod_distrito;distrito;tipo_accidente;estado_meteorológico;tipo_vehiculo;tipo_persona;rango_edad;sexo;cod_lesividad;tipo_lesividad;coordenada_x_utm;coordenada_y_utm;positiva_alcohol;positiva_droga\n"
    csv_2019_data = "2019S001;01/01/2019;10:00:00;CALLE A;1;1;CENTRO;Colisión;Despejado;Bici;Cond;20;H;3;Grave;438.315;4.478.761;N;N\n"
    with open(
        os.path.join(ruta_accidentes, "AccidentesBicicletas_2019.csv"),
        "w",
        encoding="UTF-8",
    ) as f:
        f.write(csv_2019_header + csv_2019_data)

    # Archivo 2 (2025) usa 'lesividad', ',' como decimal y no usa separador de miles
    csv_2025_header = "num_expediente;fecha;hora;localizacion;numero;cod_distrito;distrito;tipo_accidente;estado_meteorológico;tipo_vehiculo;tipo_persona;rango_edad;sexo;cod_lesividad;lesividad;coordenada_x_utm;coordenada_y_utm;positiva_alcohol;positiva_droga\n"
    csv_2025_data = "2025S001;01/01/2025;11:00:00;CALLE B;2;2;RETIRO;Caída;Nublado;Bici;Cond;30;M;1;Leve;440000,5;4470000,5;N;N\n"
    with open(
        os.path.join(ruta_accidentes, "AccidentesBicicletas_2025.csv"),
        "w",
        encoding="UTF-8",
    ) as f:
        f.write(csv_2025_header + csv_2025_data)

    yield ruta_temp

    shutil.rmtree(ruta_temp)


@pytest.fixture(scope="session")
def df_fuente_a_cargado(spark, mock_accidentes_path) -> DataFrame:
    """Fixture que ejecuta la carga de Fuente A una vez y cachea el resultado."""
    df = cargar_datos_fuente_a(spark, mock_accidentes_path)
    df.cache()
    return df


# --- Tests Modulares para Fuente A ---


def test_fuente_a_union(df_fuente_a_cargado: DataFrame):
    """Test: Verifica que la unión de múltiples CSVs de accidentes funciona."""
    assert df_fuente_a_cargado.count() == 2


def test_fuente_a_coalesce_lesividad(df_fuente_a_cargado: DataFrame):
    """Test: Verifica la unificación de 'lesividad' y 'tipo_lesividad'."""
    assert "lesividad" in df_fuente_a_cargado.columns
    assert "tipo_lesividad" not in df_fuente_a_cargado.columns

    resultados = {r.num_expediente: r.lesividad for r in df_fuente_a_cargado.collect()}
    assert resultados["2019S001"] == "Grave"  # Vino de 'tipo_lesividad'
    assert resultados["2025S001"] == "Leve"  # Vino de 'lesividad'


def test_fuente_a_limpieza_coordenadas(df_fuente_a_cargado: DataFrame):
    """Test: Verifica la limpieza de coordenadas ('.' y ',')."""
    resultados = {
        r.num_expediente: (r.coordenada_x_utm, r.coordenada_y_utm)
        for r in df_fuente_a_cargado.collect()
    }
    # 2019: Quita '.' de miles
    assert resultados["2019S001"] == (438315.0, 4478761.0)
    # 2025: Reemplaza ',' decimal
    assert resultados["2025S001"] == (440000.5, 4470000.5)


def test_fuente_a_formato_fecha_hora(df_fuente_a_cargado: DataFrame):
    """Test: Verifica la conversión de fecha (a Date) y hora (a HXX)."""
    fila_2019 = df_fuente_a_cargado.filter(col("num_expediente") == "2019S001").first()
    fila_2025 = df_fuente_a_cargado.filter(col("num_expediente") == "2025S001").first()

    assert isinstance(fila_2019.fecha, date)
    assert fila_2019.fecha == date(2019, 1, 1)
    assert fila_2019.hora == "H10"  # de "10:00:00"

    assert isinstance(fila_2025.fecha, date)
    assert fila_2025.fecha == date(2025, 1, 1)
    assert fila_2025.hora == "H11"  # de "11:00:00"


# --- Fixtures para procesar_fuente_c (Fuente C) ---
@pytest.fixture(scope="session")
def mock_meteo_path(spark):
    """Crea archivos CSV mock para probar la unión de Fuente C."""
    ruta_temp = tempfile.mkdtemp()
    ruta_meteo = os.path.join(ruta_temp, "meteo")
    os.makedirs(ruta_meteo)

    # --- INICIO DE LA CORRECCIÓN ---
    # Definir todas las columnas H/V de 1 a 24
    h_cols = [f"H{i:02d}" for i in range(1, 25)]
    v_cols = [f"V{i:02d}" for i in range(1, 25)]

    # Unir headers
    csv_header = (
        "ESTACION;MAGNITUD;ANO;MES;DIA;"
        + ";".join(f"{h};{v}" for h, v in zip(h_cols, v_cols))
        + "\n"
    )

    # --- Corrección: Generar datos nulos como "" (string vacía) y "N" ---
    # En lugar de "N;N", usamos ";N"
    # HXX (numérico) será "" -> null
    # VXX (string) será "N"  -> "N"

    # Pares de "null" para H04-H24 (21 pares)
    pairs_null = [";N"] * 21  # 21 pares de (valor nulo, validación 'N')

    # Datos de ejemplo (solo rellenamos los 3 primeros)
    pairs_data_1 = ["10.0;V", "11.0;V", "12.0;V"]
    data_1_vals = ";".join(pairs_data_1 + pairs_null)
    csv_data_1 = f"79;83;2025;1;1;{data_1_vals}\n"  # E79, Temp

    pairs_data_2 = ["1.0;V", "1.1;V", "1.2;V"]
    data_2_vals = ";".join(pairs_data_2 + pairs_null)
    csv_data_2 = f"79;81;2025;1;1;{data_2_vals}\n"  # E79, Viento

    pairs_data_3 = ["13.0;V", "14.0;V", "15.0;V"]
    data_3_vals = ";".join(pairs_data_3 + pairs_null)
    csv_data_3 = f"79;83;2025;1;2;{data_3_vals}\n"  # E79, Temp, Día 2

    with open(os.path.join(ruta_meteo, "2025_sim.csv"), "w", encoding="UTF-8") as f:
        f.write(csv_header + csv_data_1 + csv_data_2 + csv_data_3)

    # Simular un segundo archivo (ej. 2024.csv) que será unido
    pairs_data_4 = ["20.0;V", "21.0;V", "22.0;V"]
    data_4_vals = ";".join(pairs_data_4 + pairs_null)
    csv_data_4 = f"102;83;2024;12;31;{data_4_vals}\n"  # E102, Temp

    with open(os.path.join(ruta_meteo, "2024_sim.csv"), "w", encoding="UTF-8") as f:
        f.write(csv_header + csv_data_4)
    # --- FIN DE LA CORRECCIÓN ---

    yield ruta_temp

    shutil.rmtree(ruta_temp)


@pytest.fixture(scope="session")
def df_fuente_c_procesado(spark, mock_meteo_path) -> DataFrame:
    """Fixture que ejecuta el procesamiento de Fuente C una vez y cachea el resultado."""
    df = procesar_fuente_c(spark, mock_meteo_path)
    df.cache()
    # Recolectar resultados aquí para que los tests solo hagan lookups
    resultados = {
        (r.COD_ESTACION, r.FECHA, r.HORA): r
        for r in df.collect()
        if r.FECHA >= date(2024, 1, 1)  # Filtrar solo nuestros mocks
    }
    return resultados


# --- Tests Modulares para Fuente C ---


def test_fuente_c_union_archivos(df_fuente_c_procesado: dict):
    """Test: Verifica que se cargaron CSVs de ambas estaciones (79 y 102)."""
    estaciones_encontradas = {key[0] for key in df_fuente_c_procesado.keys()}
    assert 79 in estaciones_encontradas
    assert 102 in estaciones_encontradas


def test_fuente_c_pivot(df_fuente_c_procesado: dict):
    """Test 1.2: Verifica que el pivoteo (wide-long-wide) funcionó."""
    d1_2025 = date(2025, 1, 1)
    d1_2024 = date(2024, 12, 31)

    # Verificar datos de pivoteo (Estación 79)
    # (79, d1_2025, "H01") viene de T=10.0 y VV=1.0
    assert df_fuente_c_procesado[(79, d1_2025, "H01")]["T_83_t=0"] == 10.0
    assert df_fuente_c_procesado[(79, d1_2025, "H01")]["VV_81_t=0"] == 1.0
    assert df_fuente_c_procesado[(79, d1_2025, "H02")]["T_83_t=0"] == 11.0
    assert df_fuente_c_procesado[(79, d1_2025, "H02")]["VV_81_t=0"] == 1.1

    # Verificar datos de pivoteo (Estación 102)
    assert df_fuente_c_procesado[(102, d1_2024, "H01")]["T_83_t=0"] == 20.0
    assert df_fuente_c_procesado[(102, d1_2024, "H02")]["T_83_t=0"] == 21.0
    # P_89 no estaba en el mock, debe ser 0 (por na.fill)
    assert df_fuente_c_procesado[(102, d1_2024, "H02")]["P_89_t=0"] == 0.0


def test_fuente_c_lag_logic(df_fuente_c_procesado: dict):
    """Test 1.3: Verifica la lógica de ventana (lag) en Fuente C."""
    d1_2025 = date(2025, 1, 1)
    d2_2025 = date(2025, 1, 2)
    d1_2024 = date(2024, 12, 31)

    # --- Test 1: Dentro de la partición (Estación 79) ---
    # E79, D1, H01: Sin lag (rellenado con 0)
    r_h01 = df_fuente_c_procesado[(79, d1_2025, "H01")]
    assert (r_h01["T_83_t-1h"], r_h01["T_83_t-2h"]) == (0.0, 0.0)

    # E79, D1, H02: t-1h = 10.0, t-2h = 0.0
    r_h02 = df_fuente_c_procesado[(79, d1_2025, "H02")]
    assert (r_h02["T_83_t-1h"], r_h02["T_83_t-2h"]) == (10.0, 0.0)

    # E79, D1, H03: t-1h = 11.0, t-2h = 10.0
    r_h03 = df_fuente_c_procesado[(79, d1_2025, "H03")]
    assert (r_h03["T_83_t-1h"], r_h03["T_83_t-2h"]) == (11.0, 10.0)

    # E79, D2, H01: lag(1) es D1, H03 (12.0). lag(2) es D1, H02 (11.0)
    r_d2_h01 = df_fuente_c_procesado[(79, d2_2025, "H01")]
    assert (r_d2_h01["T_83_t-1h"], r_d2_h01["T_83_t-2h"]) == (12.0, 11.0)

    # --- Test 2: Entre particiones (Estación 102) ---
    # E102, D1, H01: Sin lag (partición diferente, debe ser 0)
    r_102_h01 = df_fuente_c_procesado[(102, d1_2024, "H01")]
    assert (r_102_h01["T_83_t-1h"], r_102_h01["T_83_t-2h"]) == (0.0, 0.0)

    # E102, D1, H02: t-1h = 20.0
    r_102_h02 = df_fuente_c_procesado[(102, d1_2024, "H02")]
    assert r_102_h02["T_83_t-1h"] == 20.0

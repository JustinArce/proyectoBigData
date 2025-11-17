import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, lit, concat_ws, substring
from pyspark.sql.types import IntegerType
from src.preprocesamiento import crear_variable_objetivo, cargar_datos_fuente_b
from src.cruce import cruzar_fuentes
from datetime import date
import os
import shutil
import tempfile


# Fixture de Spark
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("Pytest-ETL-Cruce")
        .master("local[2]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


# Fixture para los datos mock
@pytest.fixture(scope="session")
def mock_data(spark):
    # Mock A (Accidentes)
    df_a_raw = spark.createDataFrame(
        [
            (
                1,
                "01/01/2025",
                "01:00:00",
                10.0,
                10.0,
                4,
                "Colisión",
                "CENTRO",
                "H",
                "Despejado",
                30,  # <--- EDAD AÑADIDA
            ),  # Grave, cerca de E1. TIENE CLIMA.
            (
                2,
                "01/01/2025",
                "02:00:00",
                50.0,
                50.0,
                1,
                "Caída",
                "RETIRO",
                "M",
                "Nublado",
                45,  # <--- EDAD AÑADIDA
            ),  # Leve, cerca de E2. TIENE CLIMA.
            (
                3,
                "01/01/2025",
                "03:00:00",
                10.2,
                10.2,
                7,
                "Otro",
                "CENTRO",
                "H",
                "Despejado",
                22,  # <--- EDAD AÑADIDA
            ),  # Leve, cerca de E1. NO TIENE CLIMA.
        ],
        [
            "num_expediente",
            "fecha",
            "hora",
            "coordenada_x_utm",
            "coordenada_y_utm",
            "cod_lesividad",
            "tipo_accidente",
            "distrito",
            "sexo",
            "estado_meteorológico",
            "edad",  # <--- NOMBRE DE COLUMNA AÑADIDO
        ],
    )

    df_a = (
        crear_variable_objetivo(df_a_raw)
        .withColumn(
            "fecha",
            to_date(col("fecha"), "dd/MM/yyyy"),  # Convertir a Date
        )
        .withColumn(
            "hora",
            concat_ws(
                "H", lit(""), substring(col("hora"), 1, 2)
            ),  # "01:00:00" -> "H01"
        )
    )

    # Mock B (Estaciones)
    df_b = spark.createDataFrame(
        [
            ("E1_CERCANA", 100, 10.1, 10.1),  # Estación cercana a Accidente 1 y 3
            ("E2_LEJANA", 200, 50.1, 50.1),  # Estación cercana a Accidente 2
        ],
        [
            "COD_ESTACION_B",
            "CODIGO_CORTO",
            "COORDENADA_X_ETRS89",
            "COORDENADA_Y_ETRS89",
        ],
    )

    # Mock C (Meteo)
    df_c = spark.createDataFrame(
        [
            (
                100,
                date(2025, 1, 1),
                "H01",
                5.0,
                1.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),  # Clima para Accidente 1
            (
                200,
                date(2025, 1, 1),
                "H02",
                25.0,
                5.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),  # Clima para Accidente 2
            # Sin datos para H03 (Accidente 3)
        ],
        [
            "COD_ESTACION",
            "FECHA",
            "HORA",
            "T_83_t=0",
            "VV_81_t=0",
            "P_89_t=0",
            "T_83_t-1h",
            "VV_81_t-1h",
            "P_89_t-1h",
            "T_83_t-2h",
            "VV_81_t-2h",
            "P_89_t-2h",
        ],
    )

    return df_a, df_b, df_c


def test_spatial_join_logic(spark, mock_data):
    """
    Test 2.1: Verificar que el cruce espacial asigna la estación MÁS cercana.
    (Este test sigue siendo válido para INNER JOIN, ya que el Accidente 1 sí tiene datos)
    """
    df_a, df_b, df_c = mock_data

    # Test solo con Accidente 1
    df_a_test = df_a.filter(col("num_expediente") == 1)

    df_final = cruzar_fuentes(spark, df_a_test, df_b, df_c)
    resultado = df_final.first()

    assert resultado.num_expediente == 1
    assert (
        resultado["T_83_t=0"] == 5.0
    )  # Asegura que tomó el clima de la estación correcta


def test_join_completeness_y_schema(spark, mock_data):
    """
    Test 2.2, 2.3 y 2.4: Verificar completitud de joins (INNER) y schema final.
    """
    df_a, df_b, df_c = mock_data

    df_final = cruzar_fuentes(spark, df_a, df_b, df_c)

    # Test 2.2 (Completitud) - Deberíamos tener 2 expedientes (INNER JOIN)
    # <--- MODIFICADO: Se espera 2 en lugar de 3
    assert df_final.count() == 2

    resultados = {r.num_expediente: r for r in df_final.collect()}

    # Test 2.3 (Inner Join) - Accidente 3 no tiene clima, debe ser excluido
    # <--- MODIFICADO: Título y lógica
    assert resultados[1].num_expediente == 1
    assert resultados[1]["T_83_t=0"] == 5.0

    assert resultados[2].num_expediente == 2
    assert resultados[2]["T_83_t=0"] == 25.0

    # <--- MODIFICADO: Se comprueba que el 3 NO ESTÁ
    assert 3 not in resultados  # El Accidente 3 debe ser excluido por el INNER JOIN

    # <--- MODIFICADO: Se eliminan las aserciones para resultados[3]
    # assert resultados[3].num_expediente == 3
    # assert resultados[3]["T_83_t=0"] == 0
    # assert (
    #     resultados[3].tipo_accidente == "Otro"
    # )

    # Test 2.4 (Schema) - El schema final debe ser el mismo
    columnas_finales_esperadas = [
        "num_expediente",
        "accidente_grave",
        "T_83_t=0",
        "VV_81_t=0",
        "P_89_t=0",
        "T_83_t-1h",
        "VV_81_t-1h",
        "P_89_t-1h",
        "T_83_t-2h",
        "VV_81_t-2h",
        "P_89_t-2h",
        "tipo_accidente",
        "distrito",
        "sexo",
        "estado_meteorológico",
        "hora",
        "edad",
    ]

    for col_name in columnas_finales_esperadas:
        assert col_name in df_final.columns

import os
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
    when,
    regexp_replace,
)
from pyspark.sql.types import IntegerType, DateType, StringType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime


def cargar_datos_fuente_a(spark: SparkSession, ruta_datos: str) -> DataFrame:
    """
    Carga y unifica TODOS los CSVs de accidentes (Fuente A) de la carpeta.
    Maneja los schemas diferentes (2019 vs 2025) explícitamente.
    """
    print("Cargando Fuente A (Accidentes) desde el directorio...")

    # --- CORRECCIÓN: Leer todos los CSV del directorio ---
    # Spark unirá (append) todos los archivos.
    # inferSchema=True es necesario, pero puede no manejar la fusión de schemas (lesividad vs tipo_lesividad)
    path_a = os.path.join(ruta_datos, "accidentes/")
    df_raw = spark.read.csv(
        path_a,
        header=True,
        sep=";",
        inferSchema=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        encoding="UTF-8",
        dateFormat="dd/MM/yyyy",
    )

    # Asegurar que ambas columnas de lesividad existan antes de la unión
    if "lesividad" not in df_raw.columns:
        df_raw = df_raw.withColumn("lesividad", lit(None).cast(StringType()))

    if "tipo_lesividad" not in df_raw.columns:
        df_raw = df_raw.withColumn("tipo_lesividad", lit(None).cast(StringType()))
    # --- FIN DE LA CORRECCIÓN ---

    # Unificar (coalesce) las columnas de lesividad
    df_accidentes = (
        df_raw.withColumn(
            "lesividad_unida", coalesce(col("lesividad"), col("tipo_lesividad"))
        )
        .drop("lesividad", "tipo_lesividad")
        .withColumnRenamed("lesividad_unida", "lesividad")
    )

    # Limpieza de Coordenadas (Manejo de '.' y ',') y Fechas
    # Quitar '.' (separador de miles) y luego reemplazar ',' por '.' (decimal)
    df_accidentes_limpio = (
        df_accidentes.withColumn(
            "coordenada_x_utm",
            regexp_replace(col("coordenada_x_utm"), "\\.", "").cast(
                StringType()
            ),  # Quitar puntos
        )
        .withColumn(
            "coordenada_x_utm",
            regexp_replace(col("coordenada_x_utm"), ",", ".").cast(
                DoubleType()
            ),  # Reemplazar coma
        )
        .withColumn(
            "coordenada_y_utm",
            regexp_replace(col("coordenada_y_utm"), "\\.", "").cast(StringType()),
        )
        .withColumn(
            "coordenada_y_utm",
            regexp_replace(col("coordenada_y_utm"), ",", ".").cast(DoubleType()),
        )
        .withColumn(
            "fecha",
            to_date(col("fecha"), "dd/MM/yyyy"),  # Convertir a Date
        )
        .withColumn(
            "hora",
            concat_ws(
                "H", lit(""), substring(col("hora"), 1, 2)
            ),  # "10:00:00" -> "H10"
        )
    )

    return df_accidentes_limpio


def crear_variable_objetivo(df: DataFrame) -> DataFrame:
    """
    Test 1.1: Genera la columna binaria 'accidente_grave' (Fase 1).
    """
    print("Creando variable objetivo 'accidente_grave'...")
    df_objetivo = df.withColumn(
        "accidente_grave", when(col("cod_lesividad").isin([3, 4]), 1).otherwise(0)
    ).withColumn("accidente_grave", col("accidente_grave").cast(IntegerType()))

    return df_objetivo


def cargar_datos_fuente_b(spark: SparkSession, ruta_datos: str) -> DataFrame:
    """
    Carga y procesa la Fuente B (Estaciones Meteorológicas).
    """
    print("Cargando Fuente B (Estaciones)...")

    # Cargar el archivo CSV específico de estaciones
    path_b = os.path.join(
        ruta_datos, "estaciones/Estaciones_control_datos_meteorologicos.csv"
    )

    # --- CORRECCIÓN: Añadir encoding UTF-8 para manejar Caracteres ---
    df_b_raw = spark.read.csv(
        path_b,
        header=True,
        sep=";",
        inferSchema=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        encoding="UTF-8",  # Especificar encoding
    )
    # --- FIN DE LA CORRECCIÓN ---

    # Limpiar y castear coordenadas (reemplazar ',' por '.')
    df_b_limpio = df_b_raw.withColumn(
        "COORDENADA_X_ETRS89",
        regexp_replace(col("COORDENADA_X_ETRS89"), ",", ".").cast(DoubleType()),
    ).withColumn(
        "COORDENADA_Y_ETRS89",
        regexp_replace(col("COORDENADA_Y_ETRS89"), ",", ".").cast(DoubleType()),
    )

    # Seleccionar solo las columnas necesarias para el join
    df_estaciones = df_b_limpio.select(
        col("CODIGO").alias("COD_ESTACION_B"),  # Llave completa (ej. 28079004)
        col("CODIGO_CORTO").cast(
            IntegerType()
        ),  # Llave corta (ej. 4) -> Esta es la que une con Fuente C
        col("COORDENADA_X_ETRS89"),
        col("COORDENADA_Y_ETRS89"),
    )

    return df_estaciones


def procesar_fuente_c(spark: SparkSession, ruta_datos: str) -> DataFrame:
    """
    Carga, unifica y pivota TODOS los CSVs de Meteo (Fuente C).
    """
    print("Procesando Fuente C (Meteo)...")

    # Cargar TODOS los CSVs en el directorio de meteo
    path_c = os.path.join(ruta_datos, "meteo/")
    df_meteo_raw = spark.read.csv(
        path_c,  # Lee todos los CSVs en el directorio
        header=True,
        sep=";",
        inferSchema=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
    )

    h_cols = [f"H{i:02d}" for i in range(1, 25)]
    df_meteo_casted = df_meteo_raw
    for col_name in h_cols:
        if col_name in df_meteo_raw.columns:
            df_meteo_casted = df_meteo_casted.withColumn(
                col_name, col(col_name).cast(StringType())
            )

    # 1. Pivote (Wide-to-Long) - Test 1.2
    # Crear la expresión stack(24, 'H01', H01, 'V01', V01, ...)
    stack_expr = (
        "stack(24, "
        + ", ".join(
            [
                f"'{h}', {h}, {v}"
                for h, v in [("H%02d" % i, "V%02d" % i) for i in range(1, 25)]
            ]
        )
        + ") AS (HORA, VALOR, VALIDACION)"
    )

    df_meteo_long = df_meteo_casted.select(
        col("ESTACION"),
        col("MAGNITUD"),
        col("ANO"),
        col("MES"),
        col("DIA"),
        expr(stack_expr),
    ).filter(col("VALIDACION") == "V")  # Quedarse solo con valores validados

    # Crear columna FECHA (DateType)
    df_meteo_fecha = df_meteo_long.withColumn(
        "FECHA", expr("TO_DATE(CONCAT(ANO, '-', MES, '-', DIA), 'yyyy-M-d')")
    ).withColumn(
        "COD_ESTACION",
        col("ESTACION").cast(IntegerType()),  # Esta es la llave (ej. 4, 8, 102)
    )

    # --- CORRECCIÓN: Convertir VALOR a numérico antes de pivotar ---
    # Reemplazar ',' por '.' (para decimales europeos) y castear a Double
    df_meteo_numeric = df_meteo_fecha.withColumn(
        "VALOR", expr("CAST(REPLACE(VALOR, ',', '.') AS DOUBLE)")
    ).na.drop(subset=["VALOR"])  # Descartar filas donde el valor no era numérico

    # 2. 2do Pivote (Long-to-Column) - Test 1.2
    magnitudes_interes = [81, 83, 89]  # VV, T, P
    df_meteo_wide = (
        df_meteo_numeric.filter(col("MAGNITUD").isin(magnitudes_interes))
        .groupBy("COD_ESTACION", "FECHA", "HORA")
        .pivot("MAGNITUD", magnitudes_interes)
        .avg("VALOR")
        .withColumnRenamed("81", "VV_81_t=0")
        .withColumnRenamed("83", "T_83_t=0")
        .withColumnRenamed("89", "P_89_t=0")
        .na.fill(0)
    )  # Rellenar con 0 si una magnitud faltaba para esa hora

    # 3. Features Temporales (Lag) - Test 1.3
    window_spec = Window.partitionBy("COD_ESTACION").orderBy("FECHA", "HORA")

    df_meteo_final = (
        df_meteo_wide.withColumn(
            "T_83_t-1h", lag(col("T_83_t=0"), 1, 0).over(window_spec)
        )
        .withColumn("VV_81_t-1h", lag(col("VV_81_t=0"), 1, 0).over(window_spec))
        .withColumn("P_89_t-1h", lag(col("P_89_t=0"), 1, 0).over(window_spec))
        .withColumn("T_83_t-2h", lag(col("T_83_t=0"), 2, 0).over(window_spec))
        .withColumn("VV_81_t-2h", lag(col("VV_81_t=0"), 2, 0).over(window_spec))
        .withColumn("P_89_t-2h", lag(col("P_89_t=0"), 2, 0).over(window_spec))
    )

    return df_meteo_final

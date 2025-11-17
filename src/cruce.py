import math
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, udf, expr
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


@udf(returnType=DoubleType())
def calcular_distancia_euclidiana(x1, y1, x2, y2):
    """
    UDF para calcular la distancia euclidiana.
    Nota: Esto asume que las coordenadas UTM/ETRS89 son cartesianas
    y están en la misma proyección/zona.
    """
    if x1 is None or y1 is None or x2 is None or y2 is None:
        return float("inf")
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def cruzar_fuentes(
    spark: SparkSession,
    df_accidentes: DataFrame,
    df_estaciones: DataFrame,
    df_meteo: DataFrame,
) -> DataFrame:
    """
    Implementa la lógica de cruce espacial (A+B) y temporal (Fase 2).
    """

    print("Iniciando Fase 2.1: Cruce Espacial (A+B)...")
    # 1. Cruce Espacial (A + B) - Test 2.1
    # Usamos crossJoin, que es caro pero necesario ya que no hay llave común.
    df_distancias = df_accidentes.crossJoin(df_estaciones)

    # Calcular la distancia
    df_con_dist = df_distancias.withColumn(
        "distancia",
        calcular_distancia_euclidiana(
            col("coordenada_x_utm"),
            col("coordenada_y_utm"),
            col("COORDENADA_X_ETRS89"),
            col("COORDENADA_Y_ETRS89"),
        ),
    )

    # Seleccionar la estación más cercana (rank=1)
    window_dist = Window.partitionBy("num_expediente").orderBy("distancia")

    df_acc_con_estacion = (
        df_con_dist.withColumn("rank", row_number().over(window_dist))
        .filter(col("rank") == 1)
        .drop("rank", "distancia")
    )

    # Contar antes del join temporal
    count_pre_temporal = df_acc_con_estacion.count()
    print(f"Cruce espacial completado. Filas resultantes: {count_pre_temporal}")

    # 2. Cruce Temporal (Final) - Test 2.3
    # La llave compuesta de 3 partes (Entregable 1, pg. 2)
    # df_acc_con_estacion.CODIGO_CORTO (int) == df_meteo.COD_ESTACION (int)
    # df_acc_con_estacion.fecha (date) == df_meteo.FECHA (date)
    # df_acc_con_estacion.hora (string "HXX") == df_meteo.HORA (string "HXX")

    print("Iniciando Fase 2.2: Cruce Temporal (Final)...")
    df_final_ml = df_acc_con_estacion.join(
        df_meteo,
        [
            df_acc_con_estacion.fecha == df_meteo.FECHA,
            df_acc_con_estacion.hora == df_meteo.HORA,
            df_acc_con_estacion.CODIGO_CORTO == df_meteo.COD_ESTACION,
        ],
        "left",
    )

    df_final_ml = (
        df_final_ml.drop(df_meteo.FECHA).drop(df_meteo.HORA).drop(df_meteo.COD_ESTACION)
    )

    count_post_temporal = df_final_ml.count()
    print(f"Cruce temporal completado. Filas finales: {count_post_temporal}")
    if count_pre_temporal != count_post_temporal:
        print(
            f"WARN: Se encontraron {count_post_temporal - count_pre_temporal} duplicados o descartes inesperados."
        )

    # 3. Schema Final (Test 2.4)
    # Seleccionar solo las columnas necesarias para el modelo
    columnas_numericas = [
        "T_83_t=0",
        "VV_81_t=0",
        "P_89_t=0",
        "T_83_t-1h",
        "VV_81_t-1h",
        "P_89_t-1h",
        "T_83_t-2h",
        "VV_81_t-2h",
        "P_89_t-2h",
        "hora",
        "edad",
    ]

    columnas_categoricas = [
        "tipo_accidente",
        "distrito",
        "sexo",
        "estado_meteorológico",
        "positiva_alcohol",
        "positiva_droga",
        "tipo_vehículo",
        "tipo_persona",
    ]

    columnas_clave = ["num_expediente", "accidente_grave"]

    # Asegurarnos de que todas las columnas existan (algunas pueden faltar si el join falló)
    columnas_finales = columnas_clave.copy()

    for c in columnas_numericas:
        if c in df_final_ml.columns:
            columnas_finales.append(c)

    for c in columnas_categoricas:
        if c in df_final_ml.columns:
            columnas_finales.append(c)

    # Rellenar nulos (del left join) con 0 para features numéricas
    df_final_seleccionado = df_final_ml.select(columnas_finales)
    df_final_relleno = df_final_seleccionado.na.fill(
        0, subset=[c for c in columnas_numericas if c in columnas_finales]
    )
    # Rellenar nulos (del left join) con 'Desconocido' para categóricas
    df_final_relleno = df_final_relleno.na.fill(
        "Desconocido", subset=[c for c in columnas_categoricas if c in columnas_finales]
    )

    return df_final_relleno

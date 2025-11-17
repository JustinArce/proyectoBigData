import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, cast
from pyspark.sql.types import IntegerType
from src.preprocesamiento import (
    cargar_datos_fuente_a,
    crear_variable_objetivo,
    cargar_datos_fuente_b,
    procesar_fuente_c,
)
from src.cruce import cruzar_fuentes
from src.materializacion import materializar_datos


def main(target):
    """
    Programa principal que orquesta el pipeline de ETL
    """
    spark = (
        SparkSession.builder.appName("ETL-Accidentes-Madrid")
        .config("spark.jars", "postgresql-42.2.14.jar")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # --- Fase 1: Preprocesamiento de Fuentes ---
    print("Iniciando Fase 1: Preprocesamiento de Fuentes...")

    # Cargar y procesar Fuente A (Accidentes)
    df_a_raw = cargar_datos_fuente_a(spark, "data/")
    df_accidentes = crear_variable_objetivo(df_a_raw)

    # Cargar y procesar Fuente B (Estaciones)
    df_estaciones = cargar_datos_fuente_b(spark, "data/")

    # Cargar y procesar Fuente C (Meteo)
    df_meteo = procesar_fuente_c(spark, "data/")

    # --- Fase 2: Cruce de Fuentes ---
    print("Iniciando Fase 2: Cruce de Fuentes...")
    df_final_ml = cruzar_fuentes(spark, df_accidentes, df_estaciones, df_meteo)

    # Cachear el DF final antes de contarlo y mostrarlo
    df_final_ml.cache()

    print(f"Dataset final de ML generado con {df_final_ml.count()} filas.")
    print("Schema final:")
    df_final_ml.printSchema()
    df_final_ml.show(5)

    # --- Fase 2: Materialización ---
    print(f"Iniciando Fase 2: Materialización en '{target}'...")
    materializar_datos(df_final_ml, target, "dataset_final_ml")
    print("Materialización completada.")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ejecutar pipeline de ETL para proyecto Big Data."
    )
    parser.add_argument(
        "--target",
        type=str,
        choices=["local", "azure"],
        required=True,
        help="El destino de materialización ('local' para PostgreSQL, 'azure' para ADLS)",
    )
    args = parser.parse_args()
    main(args.target)

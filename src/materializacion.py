import os
from pyspark.sql import DataFrame


def materializar_datos(df: DataFrame, nombre_tabla: str):
    """
    Escribe el DataFrame final en el destino especificado (PostgreSQL o ADLS).
    """
    print(f"Materializando en PostgreSQL local (tabla: {nombre_tabla})...")

    # Conexión via IP del gateway de Docker
    jdbc_url = "jdbc:postgresql://172.17.0.1:5433/postgres"
    db_properties = {
        "user": "postgres",
        "password": "testPassword",
        "driver": "org.postgresql.Driver",
    }

    df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", nombre_tabla
    ).option("user", db_properties["user"]).option(
        "password", db_properties["password"]
    ).option("driver", db_properties["driver"]).option("encoding", "UTF-8").mode(
        "overwrite"
    ).save()

    print(f"Datos escritos en PostgreSQL, tabla '{nombre_tabla}'.")

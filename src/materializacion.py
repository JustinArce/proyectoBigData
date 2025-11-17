import os
from pyspark.sql import DataFrame


def materializar_datos(df: DataFrame, target: str, nombre_tabla: str):
    """
    Escribe el DataFrame final en el destino especificado (PostgreSQL o ADLS).
    """
    if target == "local":
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

    elif target == "azure":
        print(
            f"Materializando en Azure Data Lake (ADLS) como '{nombre_tabla}.parquet'..."
        )
        # (Aquí iría la lógica de autenticación y escritura en ADLS)
        # ej. df.write.parquet(f"azureml://datastores/workspaceblobstore/paths/proyecto_accidentes/{nombre_tabla}.parquet")
        print("Datos escritos en ADLS (simulado).")

    else:
        raise ValueError(f"Target '{target}' no reconocido.")

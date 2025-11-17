#!/bin/bash
#
# PASO 3: Ejecuta el ETL localmente (en Docker) y sube el resultado (Parquet) a Azure.
#
set -e

# --- Configuración (EDITAR ESTO) ---
RESOURCE_GROUP="rg-bigdata-proyecto"
WORKSPACE_NAME="aml-bigdata-ws"
DATA_ASSET_NAME="dataset_final_ml"
DATA_ASSET_VERSION="1"
LOCAL_OUTPUT_DIR="./data_output"
# -----------------------------------

# 1. Construir la imagen de Docker (necesaria para ejecutar el ETL)
echo "Construyendo imagen 'bigdata'..."
docker build -t bigdata .

# 2. Ejecutar el ETL para generar el Parquet local
echo "Ejecutando ETL (Fases 1 y 2) dentro de Docker..."
# Montamos el directorio actual para que 'etl.py' pueda escribir en './data_output'
docker run --rm \
  -v $(pwd):/src \
  -w /src \
  bigdata python etl.py --target local-parquet

echo "ETL completado. Parquet generado en $LOCAL_OUTPUT_DIR"

# 3. Subir el Parquet a Azure como un Data Asset
echo "Creando Data Asset '$DATA_ASSET_NAME:$DATA_ASSET_VERSION' en Azure..."

# 3a. Crear la definición del Data Asset
cat <<EOF > azure-data-def.yml
\$schema: https://azuremlschemas.azureedge.net/latest/data.schema.json
name: $DATA_ASSET_NAME
version: $DATA_ASSET_VERSION
type: uri_folder
path: $LOCAL_OUTPUT_DIR/dataset_final_ml.parquet/
description: "Dataset de accidentes y clima procesado y cruzado."
EOF

# 3b. Subir los datos y registrar el Asset
az ml data create --file azure-data-def.yml \
  --resource-group $RESOURCE_GROUP \
  --workspace-name $WORKSPACE_NAME

rm azure-data-def.yml

echo "--- ¡Datos subidos y registrados en Azure! ---"
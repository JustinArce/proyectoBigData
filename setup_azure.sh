#!/bin/bash
#
# PASO 1: Configura la infraestructura de Azure (Workspace, Cómputo, Entorno).
# Se ejecuta UNA SOLA VEZ.
#
set -e

# --- Configuración (EDITAR ESTO) ---
RESOURCE_GROUP="rg-bigdata-proyecto"
LOCATION="eastus"
WORKSPACE_NAME="aml-bigdata-ws"
COMPUTE_NAME="cpu-cluster"
ENV_NAME="bigdata-env"
ENV_VERSION="1"
# -----------------------------------

echo "Iniciando sesión en Azure..."
az login

echo "Creando Grupo de Recursos: $RESOURCE_GROUP"
az group create --name $RESOURCE_GROUP --location $LOCATION

echo "Creando AML Workspace: $WORKSPACE_NAME"
az ml workspace create --name $WORKSPACE_NAME --resource-group $RESOURCE_GROUP

echo "Creando Clúster de Cómputo: $COMPUTE_NAME"
az ml compute create --name $COMPUTE_NAME \
  --resource-group $RESOURCE_GROUP \
  --workspace-name $WORKSPACE_NAME \
  --type AmlCompute \
  --size Standard_DS11_v2 \
  --min-instances 0 \
  --max-instances 10 \
  --idle-time-before-scale-down 600

echo "Creando Entorno $ENV_NAME:$ENV_VERSION desde Dockerfile..."
# 1. Crear la definición del entorno en un archivo YAML
cat <<EOF > azure-env-def.yml
\$schema: https://azuremlschemas.azureedge.net/latest/environment.schema.json
name: $ENV_NAME
version: $ENV_VERSION
build:
  path: .
  dockerfile_path: Dockerfile
EOF

# 2. Registrar el entorno en Azure (esto construirá la imagen de Docker)
az ml environment create --file azure-env-def.yml \
  --resource-group $RESOURCE_GROUP \
  --workspace-name $WORKSPACE_NAME

rm azure-env-def.yml

echo "--- ¡Configuración de Azure completada! ---"
echo "Workspace: $WORKSPACE_NAME"
echo "Cómputo: $COMPUTE_NAME"
echo "Entorno: $ENV_NAME:$ENV_VERSION"
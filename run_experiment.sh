#!/bin/bash
#
# PASO 4: Lanza la ejecución paralela del experimento (Fase 3) en Azure.
#
set -e

# --- Configuración (EDITAR ESTO) ---
RESOURCE_GROUP="rg-bigdata-proyecto"
WORKSPACE_NAME="aml-bigdata-ws"
# -----------------------------------

# 1. Generar los 18 archivos YAML de trabajos
echo "Generando 18 archivos de trabajo YAML..."
cd parallel
python generate_jobs.py
cd ..

# 2. Enviar los 18 trabajos a Azure
echo "Enviando trabajos a Azure (ejecución en paralelo)..."

# Iterar sobre todos los archivos .yml en el directorio jobs/
for job_file in parallel/jobs/*.yml; do
    echo "Enviando trabajo: $job_file"
    az ml job create --file "$job_file" \
      --resource-group $RESOURCE_GROUP \
      --workspace-name $WORKSPACE_NAME \
      --stream=false
    # --stream=false para no bloquear la terminal
done

echo "--- ¡Todos los trabajos han sido enviados! ---"
echo "Monitorea el progreso en MLflow (Azure Studio) o en 'notebooks/Analisis_Resultados.ipynb'"
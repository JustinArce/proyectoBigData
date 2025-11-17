#!/bin/bash
#
# Lanzador Paralelo para Azure (Fase 3)
# Esto itera sobre todos los archivos YAML en el directorio 'parallel/'
# y los envía como trabajos a Azure Machine Learning.
#

echo "Iniciando Lanzador de Experimento Paralelo (Azure)..."

# Asumir que el CLI de Azure 'az' está logueado y configurado.

# Directorio que contiene los 18 archivos YAML
JOB_DIR="parallel"

if [ ! -d "$JOB_DIR" ]; then
  echo "Error: Directorio '$JOB_DIR' no encontrado."
  echo "Por favor, asegúrate de que los 18 archivos YAML de trabajo estén en la carpeta 'parallel/'."
  exit 1
fi

# Iterar sobre todos los archivos YAML en el directorio y lanzarlos
for JOB_FILE in "$JOB_DIR"/job-*.yml; do
  if [ -f "$JOB_FILE" ]; then
    echo "==> Enviando trabajo a Azure: $JOB_FILE"
    az ml job create --file "$JOB_FILE"
    
    if [ $? -ne 0 ]; then
      echo "ERROR: Falló el envío del trabajo $JOB_FILE."
      # Opcional: decidir si abortar o continuar con los demás
      # exit 1 
    else
      echo "Trabajo $JOB_FILE enviado exitosamente."
    fi
  fi
done

echo "Todos los 18 trabajos han sido enviados a Azure."
echo "Puedes monitorear el progreso en el portal de Azure ML."
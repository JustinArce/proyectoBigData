!/bin/bash
# Script para enviar todos los trabajos L9 a Azure ML

# Asegurarse de que los YAMLs existen
if [ ! -d "jobs" ]; then
    echo "Directorio 'jobs' no encontrado. Ejecutando generador..."
    python generate_jobs.py
fi

echo "Iniciando envío masivo de 18 trabajos a Azure ML..."

# Iterar sobre todos los archivos .yml en el directorio jobs/
for job_file in jobs/*.yml; do
    echo "Enviando trabajo: $job_file"
    az ml job create --file "$job_file" --stream=false
    # --stream=false para no bloquear la terminal esperando a que termine cada uno
done

echo "¡Todos los trabajos han sido enviados! Monitorea el progreso en MLflow o Azure Studio."
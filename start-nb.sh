#!/bin/bash
# Script Para iniciar el contenedor de bigdata y ejecutar Jupyter Notebook

echo "--- Detener contenedor bigdata... ---"
docker stop bigdata 2>/dev/null || true

echo "--- Detener y eliminar contenedor bigdata-db... ---"
docker stop bigdata-db 2>/dev/null || true
docker rm bigdata-db 2>/dev/null || true

echo "--- Construir imagen 'bigdata'... ---"
docker build -t bigdata .

echo "--- Iniciar contenedor 'bigdata-db'... ---"
docker run --name bigdata-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres

echo "--- Iniciando contenedor 'bigdata' en bash... ---"
docker run --rm -p 8888:8888 -i -t bigdata /bin/bash -c "jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root; /bin/bash"
# Proyecto Big Data: Predicción de Gravedad de Accidentes de Bicicleta

Este proyecto implementa un pipeline de Big Data completo (ETL + Machine Learning) para predecir la gravedad de accidentes de bicicleta en Madrid, integrando datos de accidentes, estaciones de control y meteorología histórica.

El sistema utiliza **Apache Spark**, **PostgreSQL**, **MLflow** y **Optuna**, todo encapsulado en un entorno **Docker**.

---

## 🚀 1. Instalación y Despliegue del Entorno

Siga estos pasos para configurar y ejecutar el proyecto completo en su máquina local.

### Paso 1.1: Clonar el Repositorio

Primero, clone este repositorio de GitHub en su máquina local.

```bash
git clone https://github.com/J
cd proyectoBigDataJA
```

### Paso 1.2: Requisitos Previos

* **Docker** instalado y corriendo en su máquina.
* **Linux** o **WSL**.


### Paso 1.3: Construir y Ejecutar Contenedores

El script `start_local_env.sh` automatiza todo el despliegue del entorno:
1.  Construye la imagen de Docker (`bigdata`) a partir del `Dockerfile`.
2.  Inicia el contenedor de base de datos (`bigdata-db` con PostgreSQL).
3.  Inicia el contenedor principal (`bigdata`) y abre una terminal interactiva (`bash`).

Desde la raíz del proyecto, ejecute:

```bash
bash start.sh
```

> **Nota:** Al finalizar la ejecución del script, usted estará **dentro** de la terminal del contenedor (`bash-5.0#` o similar). **Todos los pasos siguientes se ejecutan dentro de esta terminal.**

---

## 🧪 2. Ejecución de Pruebas Unitarias

Antes de procesar los datos, validamos la lógica de transformación y cruce. El proyecto utiliza `pytest` para validar el preprocesamiento (limpieza, pivoteo) y la lógica de unión (espacial y temporal).

Ejecute el siguiente comando dentro del contenedor:

```bash
pytest
```

**Resultado esperado:** Verá una salida verde indicando que los tests en `tests/test_preprocesamiento.py` y `tests/test_cruce.py` han pasado exitosamente.

---

## ⚙️ 3. Ejecución del Pipeline ETL (Fases 1 y 2)

Este es el programa principal (`etl.py`) que orquesta la carga de datos, la limpieza, el cruce de fuentes y la materialización en la base de datos.

Ejecute el script ETL especificando el destino local:

```bash
python etl.py
```

**Lo que sucede internamente:**
1.  **Fase 1:** Carga y unifica múltiples CSVs de `datos/accidentes/` y `datos/meteo/`.
2.  **Fase 2:** Realiza un *Cruce Espacial* (distancia euclidiana) y un *Cruce Temporal* (`inner join` con clima).
3.  **Materialización:** Escribe la tabla final `dataset_final_ml` en la base de datos PostgreSQL `accidentes_db`.

---

## 🗄️ 4. Estructura de la Base de Datos (Schema)

Una vez ejecutado el ETL, los datos se almacenan en la tabla `dataset_final_ml` en PostgreSQL. A continuación se detalla su estructura para facilitar la consulta y validación.

### 4.1 Esquema de la Tabla `dataset_final_ml`

La tabla contiene una fila por cada accidente que pudo ser cruzado exitosamente con una estación meteorológica y datos climáticos.

**Columnas Principales:**
* **`num_expediente`** (string): Identificador único del accidente.
* **`accidente_grave`** (integer): **Variable Objetivo**. `1` (Grave/Fatal), `0` (Leve).
* **`T_83_t=0`** (double): Temperatura actual (ºC).
* **`VV_81_t=0`** (double): Velocidad del Viento actual (m/s).
* **`P_89_t=0`** (double): Precipitación actual (l/m²).
* **Variables Temporales (Lag):** `T_83_t-1h`, `T_83_t-2h`, etc. (Estado del clima 1 y 2 horas antes).
* **Variables Categóricas:** `hora`, `tipo_accidente`, `distrito`, `sexo`, `estado_meteorológico`.

**Esquema Técnico (Spark):**
```text
 |-- num_expediente: string (nullable = true)
 |-- accidente_grave: integer (nullable = true)
 |-- T_83_t=0: double (nullable = true)
 |-- VV_81_t=0: double (nullable = true)
 |-- P_89_t=0: double (nullable = true)
 |-- T_83_t-1h: double (nullable = true)
 |-- VV_81_t-1h: double (nullable = true)
 |-- P_89_t-1h: double (nullable = true)
 |-- T_83_t-2h: double (nullable = true)
 |-- VV_81_t-2h: double (nullable = true)
 |-- P_89_t-2h: double (nullable = true)
 |-- hora: string (nullable = true)
 |-- tipo_accidente: string (nullable = true)
 |-- distrito: string (nullable = true)
 |-- sexo: string (nullable = true)
 |-- estado_meteorológico: string (nullable = true)
 |-- positiva_alcohol: string (nullable = true)
 |-- positiva_droga: string (nullable = true)
 |-- tipo_persona: string (nullable = true)
```

### 4.2 Muestra de Datos (Output Real)

Ejemplo de registros almacenados en la base de datos, mostrando la correcta integración y codificación de caracteres:

```text
+--------------+---------------+--------+---------+--------+---------+----------+---------+---------+----------+---------+----+--------------------+-------------------+------+--------------------+----------------+--------------+------------+
|num_expediente|accidente_grave|T_83_t=0|VV_81_t=0|P_89_t=0|T_83_t-1h|VV_81_t-1h|P_89_t-1h|T_83_t-2h|VV_81_t-2h|P_89_t-2h|hora|      tipo_accidente|           distrito|  sexo|estado_meteorológico|positiva_alcohol|positiva_droga|tipo_persona|
+--------------+---------------+--------+---------+--------+---------+----------+---------+---------+----------+---------+----+--------------------+-------------------+------+--------------------+----------------+--------------+------------+
|   2019S002594|              0|    -1.3|      0.0|     0.0|     -0.1|       0.0|      0.0|      1.9|       0.0|      0.0| H22|Choque contra obs...|             LATINA|Hombre|           Despejado|               N|          NULL|   Conductor|
|   2019S006584|              0|    -1.3|      0.0|     0.0|     -0.1|       0.0|      0.0|      1.9|       0.0|      0.0| H22|Colisión fronto-l...|        CARABANCHEL|Hombre|           Despejado|               N|          NULL|   Conductor|
|   2019S002591|              0|    -1.3|      0.0|     0.0|     -0.1|       0.0|      0.0|      1.9|       0.0|      0.0| H22|Choque contra obs...|        CARABANCHEL|Hombre|           Despejado|               N|          NULL|   Conductor|
|   2020S001062|              0|    11.9|      0.0|     0.0|     10.5|       0.0|      0.0|      9.0|       0.0|      0.0| H16| Atropello a persona|           CHAMBERÍ|Hombre|           Despejado|               N|          NULL|   Conductor|
|   2019S001738|              0|     9.0|      0.0|     0.0|      3.3|       0.0|      0.0|     -0.6|       0.0|      0.0| H13|               Caída|FUENCARRAL-EL PARDO|Hombre|           Despejado|               N|          NULL|   Conductor|
+--------------+---------------+--------+---------+--------+---------+----------+---------+---------+----------+---------+----+--------------------+-------------------+------+--------------------+----------------+--------------+------------+
```

### 4.3 Verificación Manual con SQL

Puede conectarse a la base de datos para ejecutar sus propias consultas de validación:

```bash
# Desde la terminal del contenedor:
psql -h 172.17.0.1 -p 5433 -U postgres -d accidentes_db
# Contraseña: testPassword
```

**Consultas sugeridas:**
```sql
SELECT count(*) FROM dataset_final_ml;
SELECT accidente_grave, count(*) FROM dataset_final_ml GROUP BY accidente_grave;
SELECT AVG("T_83_t=0") as temp_media FROM dataset_final_ml;
```

---

## 📓 5. Experimentación y Análisis (Fases 3-5)

Una vez que los datos están en la base de datos, procedemos con el Análisis Exploratorio (EDA) y el Diseño de Experimentos (DOE).

### Paso 5.1: Iniciar Jupyter

Dentro de la terminal del contenedor, inicie el servidor de Jupyter:

```bash
jupyter notebook --ip 0.0.0.0 --no-browser --allow-root
```

### Paso 5.2: Abrir el Notebook

1.  Copie la URL que aparece en la terminal (algo como `http://127.0.0.1:8888/?token=...`).
2.  Ábrala en su navegador web.
3.  Navegue a la carpeta `notebooks/` y abra el archivo:
    * **`ProyectoCompleto_BigData.ipynb`**

### Paso 5.3: Ejecutar el Flujo

Ejecute las celdas del notebook en orden. El flujo cubre:
* **Fase 3 (EDA):** Carga de datos desde PostgreSQL y análisis de correlaciones.
* **Fase 4 (DOE):** Ejecución de 12 experimentos controlados (Taguchi L12) usando **MLflow** local (en `./mlruns`) para rastrear resultados. Compara estrategias de desbalanceo (`Weights`, `RUS`, `ROS`).
* **Fase 5 (Duelo Final):** Selección automática del mejor "Campeón", re-optimización, y evaluación final en el conjunto de test.

---

## 📂 Estructura del Proyecto

```text
.
├── datos/                  # Archivos CSV fuente (Accidentes, Estaciones, Meteo) y Metadatos
├── notebooks/              # Jupyter Notebooks para experimentación
│   └── ProyectoCompleto_BigData.ipynb
├── src/                    # Código fuente modular (ETL)
│   ├── preprocesamiento.py
│   ├── cruce.py
│   └── materializacion.py
├── tests/                  # Pruebas unitarias (pytest)
├── etl.py                  # Script orquestador principal
├── Dockerfile              # Definición de la imagen
├── start.sh      # Script de arranque del entorno
├── README.md               # Instrucciones de ejecución
└── postgresql-42.2.14.jar  # driver PSQL
```
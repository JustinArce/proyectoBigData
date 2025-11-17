import os

# --- CONFIGURACIÓN AZURE (EDITAR ESTO) ---
AZURE_COMPUTE = "azureml:cpu-cluster"  # Nombre de tu clúster en Azure
AZURE_ENV = "azureml:bigdata-env:1"    # Nombre y versión de tu entorno registrado
DATA_PATH = "azureml://datastores/workspaceblobstore/paths/proyecto_accidentes/dataset_final_ml" # Ruta en ADLS
EXPERIMENT_NAME = "Proyecto_Accidentes_DOE"

# --- MATRIZ ORTOGONAL L9 (3^3) ---
# Niveles: 0, 1, 2
L9_INDICES = [
    (0, 0, 0), # Run 1
    (0, 1, 1), # Run 2
    (0, 2, 2), # Run 3
    (1, 0, 1), # Run 4
    (1, 1, 2), # Run 5
    (1, 2, 0), # Run 6
    (2, 0, 2), # Run 7
    (2, 1, 0), # Run 8
    (2, 2, 1), # Run 9
]

# Mapeo de Factores a Valores Reales (Según PlanDeProyectoDefinitivo.md)
FACTORS = {
    "B": ["Weights", "RUS", "None"],       # Desbalanceo
    "C": ["Drop", "OHE", "Hasher"],        # Categórica
    "D": ["t=0", "t-1h", "t-2h"]           # Temporal
}

MODELS = ["LR", "GBT"]

def generate_yaml(filename, modelo, b_val, c_val, d_val, run_id):
    yaml_content = f"""$schema: https://azuremlschemas.azureedge.net/latest/commandJob.schema.json
code: ../
command: >-
  python train.py 
  --modelo {modelo} 
  --desbalanceo {b_val} 
  --categorica {c_val} 
  --temporal "{d_val}"
environment: {AZURE_ENV}
compute: {AZURE_COMPUTE}
experiment_name: {EXPERIMENT_NAME}
display_name: run_{modelo}_L9_{run_id}
description: "Modelo {modelo}, B={b_val}, C={c_val}, D={d_val}"
environment_variables:
  DATA_INPUT_PATH: {DATA_PATH}
"""
    with open(filename, "w") as f:
        f.write(yaml_content)
    print(f"Generado: {filename}")

def main():
    # Crear directorio si no existe
    if not os.path.exists("jobs"):
        os.makedirs("jobs")

    for modelo in MODELS:
        print(f"--- Generando trabajos para {modelo} ---")
        for i, (b_idx, c_idx, d_idx) in enumerate(L9_INDICES):
            run_id = i + 1
            b_val = FACTORS["B"][b_idx]
            c_val = FACTORS["C"][c_idx]
            d_val = FACTORS["D"][d_idx]
            
            filename = f"jobs/job-{modelo.lower()}-{run_id}.yml"
            generate_yaml(filename, modelo, b_val, c_val, d_val, run_id)

if __name__ == "__main__":
    main()
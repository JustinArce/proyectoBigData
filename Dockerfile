FROM manu3193/bigdata

# --- Adiciones para el Plan de Proyecto (MLflow, Optuna, Azure) ---
RUN pip3 install mlflow optuna

WORKDIR /src

COPY . /src

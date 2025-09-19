FROM apache/airflow:2.10.5

USER root

# Копируем DAGs, SQL и plugins внутрь контейнера
COPY dags/ /opt/airflow/dags/
COPY sql/ /opt/airflow/dags/sql/
COPY plugins/ /opt/airflow/plugins/

# Рабочая директория
WORKDIR /opt/airflow

# Права
RUN chown -R airflow: /opt/airflow

# Копируем entrypoint
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

USER airflow

#!/bin/bash
set -e

# Ждём пока Postgres поднимется
echo "Waiting for Postgres..."
until pg_isready -h postgres -p 5432 -U airflow; do
  sleep 2
done

echo "Postgres is ready"

# Создаём пользователя для DAG’ов, если ещё не созданы
psql "postgresql://airflow:airflow@postgres:5432/postgres" -c "CREATE USER airflow WITH PASSWORD 'airflow';" || true
psql "postgresql://airflow:airflow@postgres:5432/postgres" -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO airflow;" || true

# Инициализация Airflow DB
echo "Initializing Airflow DB..."
airflow db init


# Создание администратора, если его нет
airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow || true

# Создание подключения
airflow connections add 'postgres_conn' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'postgres' \
    --conn-port 5432 || true

# Запуск Airflow в SequentialExecutor режиме
echo "Starting Airflow..."
exec airflow standalone

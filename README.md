# Инструкция по установке

Как поднять airflow+postgres?

---

## Требования

* Docker + docker compose (Linux/macOS/Windows).
Для Windows рекомендуется WSL2 или Docker Desktop.

## 1. Клонировать репозиторий

```bash
git clone https://github.com/VasMr/DataVault2.0.git
cd DataVault2.0
```


## 2. Запуск (Linux / macOS / WSL)

```bash
# в корне проекта
chmod +x entrypoint.sh || true
docker compose up --build -d
```

## 2.1 Запуск (Windows + Docker Desktop без WSL)

Открыть PowerShell в каталоге проекта и выполнить:

```powershell
dos2unix entrypoint.sh || true
docker compose up --build -d
```

## 3. Остановить / удалить

```bash
docker compose down
# удалить данные Postgres
docker compose down -v
```

## Параметры подключения

### Postgres (DBeaver)

* Host: `localhost`
* Port: `5432`
* Database: `postgres`
* User: `airflow`
* Password: `airflow`

### Airflow UI

* URL: `http://localhost:8080`
* Username: `airflow`
* Password: `airflow`

## Что важно

* `entrypoint.sh` ждёт Postgres, инициализирует БД, создаёт пользователя и подключение, затем запускает `airflow standalone`.
* В `docker-compose.yml` Postgres проброшен на `5432:5432` — поэтому `localhost:5432` работает с хоста.

## Быстрая отладка

```bash
# посмотреть статусы
docker compose ps
# логи
docker compose logs -f airflow
docker compose logs -f postgres
# зайти в контейнер airflow
docker compose exec airflow bash
```

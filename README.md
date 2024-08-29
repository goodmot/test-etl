# test-etl
Играюсь с Airflow в докере.

Для запуска Airflow в докере есть [инструкция](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

## Основные команды
Инициализировать базу данных:
```
docker compose up airflow-init
```
Запуск всех служб:
```
docker compose up
```
Очистка мусора:
```
docker compose down --volumes --rmi all
```
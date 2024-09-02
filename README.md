# test-etl
Играюсь с Airflow в докере.

Для начала надо сделать виртуальное окружение.
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

## Материалы
Про Airflow provider и ClickhouseOperator можно почитать [тут](https://github.com/bryzgaloff-pypi/apache-airflow-providers-clickhouse) и  [тут](https://bigdataschool.ru/blog/news/airflow/clickhouse-airflow-integration.html).
Также есть полезная информация про [airflow-clickhouse-plugin](https://github.com/bryzgaloff/airflow-clickhouse-plugin), но здесь он не использовался.


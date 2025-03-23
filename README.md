# My Airflow Project

## Описание

Этот проект выполняет мониторинг консистентности данных для системы Data360, извлекая данные из базы данных, генерируя отчеты и отправляя их по электронной почте.

## Требования

1. Установите Python 3.8+.
2. Создайте виртуальное окружение:

    ```bash
    python -m venv venv
    ```

3. Установите зависимости:

    ```bash
    pip install -r requirements.txt
    ```

4. Настройте переменные окружения (в `config/airflow_config.env`) для вашей базы данных и SMTP.

## Запуск

1. Запустите Airflow:

    ```bash
    airflow webserver -p 8080
    airflow scheduler
    ```

2. Откройте веб-интерфейс Airflow на `http://localhost:8080` и запустите ваш DAG.

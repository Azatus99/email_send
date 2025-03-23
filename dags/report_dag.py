import os
import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_db_connection():
    """Получение подключения к базе данных из переменных окружения"""
    db_params = {
        'database': os.getenv('DB_NAME', 'your_database'),  # Замените на ваши значения
        'user': os.getenv('DB_USER', 'your_username'),  # Замените на ваши значения
        'password': os.getenv('DB_PASSWORD', 'your_password'),  # Замените на ваши значения
        'host': os.getenv('DB_HOST', 'your_host'),  # Замените на ваши значения
        'port': os.getenv('DB_PORT', '5432')  # Замените на ваш порт
    }
    conn = psycopg2.connect(**db_params)
    return conn

def fetch_data_from_db(conn, query):
    """Выполнение SQL-запроса и извлечение данных"""
    return pd.read_sql_query(query, conn)

def save_reports_to_excel(totals, folder_path='reports'):
    """Сохранение итогов в Excel"""
    os.makedirs(folder_path, exist_ok=True)
    today = datetime.today().strftime('%Y%m%d')
    file_path = f"{folder_path}/Data360_KPIs_{today}.xlsx"
    with pd.ExcelWriter(file_path) as writer:
        for essence, info in totals.items():
            for kpi, insight in info.items():
                for sheet_name, data in insight.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
    return file_path

def send_email_report(file_path, to_email_list):
    """Отправка отчета на email"""
    smtp_params = {
        'server': 'smtp.yandex.ru',
        'port': 465,
        'username': os.getenv('SMTP_USER'),  # Берем SMTP данные из переменных окружения
        'password': os.getenv('SMTP_PASSWORD')
    }

    from_email = os.getenv('FROM_EMAIL')  # Email отправителя из переменной окружения
    subject = 'Data360 Monitoring Report'
    body = f'Hello!\n\nPlease find the Data360 consistency check report attached for {datetime.today().strftime("%d.%m.%Y")}.'

    message = MIMEMultipart()
    message['From'] = from_email
    message['To'] = ", ".join(to_email_list)
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    with open(file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(file_path)}")
        message.attach(part)

    with smtplib.SMTP_SSL(smtp_params['server'], smtp_params['port']) as server:
        server.login(smtp_params['username'], smtp_params['password'])
        server.sendmail(from_email, to_email_list, message.as_string())

def main_task(**kwargs):
    """Основная задача, выполняющая все действия"""
    try:
        # Подключаемся к базе данных
        conn = get_db_connection()

        # Выполняем запросы для извлечения данных (замените на реальные запросы)
        queries = ["your_sql_query_here"]  # Замените на реальные SQL-запросы
        results = [fetch_data_from_db(conn, query) for query in queries]

        # Сохраняем отчет в Excel
        file_path = save_reports_to_excel(results)

        # Отправляем отчет по электронной почте
        send_email_report(file_path, to_email_list=['email@example.com'])

    except Exception as e:
        print(f"Error occurred: {e}")

def create_dag(dag_id, schedule_interval, default_args):
    """Создание DAG для Airflow"""
    with DAG(dag_id, default_args=default_args, schedule_interval=schedule_interval) as dag:
        task = PythonOperator(
            task_id='main_task',
            python_callable=main_task,
            provide_context=True
        )
    return dag

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 23),
}

# Создаем экземпляр DAG
dag = create_dag('data360_report_dag', '@daily', default_args)

# Daily Email Project

## Description

This project performs data consistency monitoring for the system. It extracts data from the database, generates reports, and sends them via email.

## Requirements

1. Install Python 3.8+.
2. Create a virtual environment:

    ```bash
    python -m venv venv
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

4. Set up environment variables (in `config/airflow_config.env`) for your database and SMTP credentials.

## Setup

### Setting Up Environment Variables

Create a file `config/airflow_config.env` with the following content:

```bash
# Database credentials
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432

# SMTP credentials
SMTP_USER=your_smtp_user
SMTP_PASSWORD=your_smtp_password
FROM_EMAIL=your_email@example.com

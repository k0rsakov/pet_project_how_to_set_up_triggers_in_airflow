# Python 3.12.8
FROM apache/airflow:2.10.4-python3.12

# Установка пакетов разработки
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка основных пакетов
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow==2.10.4 \
    duckdb==1.4.0 \
    Faker==38.0.0 \

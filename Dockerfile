FROM apache/airflow:2.2.3

USER root

COPY requirements.txt requirements.txt

COPY dags dags

USER airflow

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN /usr/local/bin/python -m pip install -r requirements.txt


FROM apache/airflow:2.2.3

USER root

COPY requirements.txt requirements.txt

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

RUN update-alternatives --set python /usr/bin/python3.7

COPY dags dags

USER airflow

RUN pip install --upgrade pip
RUN pip install -r requirements.txt


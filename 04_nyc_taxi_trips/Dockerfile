FROM apache/airflow:3.0.4

ENV AIRFLOW_HOME=/opt/airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID
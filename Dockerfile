# official airflow image
FROM apache/airflow:3.0.6

# workdir setup
WORKDIR /opt/airflow

# actual pip to install current wheels and requirements
COPY requirements.txt ./

RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# copy dags, plugin, scripts, ge
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY scripts/ /opt/airflow/scripts/
COPY include/ /opt/airflow/include/
#COPY great_expectations/ /opt/airflow/great_expectations/

# rights
USER ${AIRFLOW_UID:-50000}

# innit command
#CMD ["airflow", "webserver"]

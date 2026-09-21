FROM python:3.12.12-slim-bookworm AS pipeline
RUN python -m venv /opt/pipeline-venv
COPY requirements.txt /tmp/requirements.txt
COPY requirements.lock /tmp/requirements.lock
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt -c /tmp/requirements.lock

FROM apache/airflow:3.1.7-python3.12
USER root
COPY --from=pipeline /opt/pipeline-venv /opt/pipeline-venv
COPY --chown=airflow:root lakehouse /opt/project/lakehouse
COPY --chown=airflow:root config /opt/project/config
COPY --chown=airflow:root dbt /opt/project/dbt
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root infra/airflow/init.sh /opt/project/airflow-init.sh
RUN cp /opt/project/dbt/profiles.yml.example /opt/project/dbt/profiles.yml \
    && mkdir -p /opt/airflow/artifacts \
    && chown airflow:root /opt/airflow/artifacts \
    && chmod -R g+rwX /opt/airflow/artifacts
ENV PYTHONPATH=/opt/project
ENV PROJECT_CONFIG=/opt/project/config/projects.yml
USER airflow
# FAB and the standard provider ship in this pinned official image.
RUN python -c "from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager; from airflow.sdk import DAG"

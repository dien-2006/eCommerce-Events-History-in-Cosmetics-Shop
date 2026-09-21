FROM apache/superset:6.0.0
USER root
COPY requirements.txt /app/requirements-lakehouse.txt
RUN . /app/.venv/bin/activate && uv pip install -r /app/requirements-lakehouse.txt
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init /app/docker-init
RUN chmod +x /app/docker-init/superset_init.sh
USER superset
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
CMD ["/bin/bash", "/app/docker-init/superset_init.sh"]

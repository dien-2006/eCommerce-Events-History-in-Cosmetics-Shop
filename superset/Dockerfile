FROM apache/superset:6.0.0

USER root
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# init scripts
COPY init/superset_init.sh /app/docker-init/superset_init.sh
RUN chmod +x /app/docker-init/superset_init.sh

USER superset

ENV PYTHONPATH=/app/pythonpath
CMD ["/app/docker-init/superset_init.sh"]
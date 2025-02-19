FROM apache/superset:latest

USER root
# Optionally install extra packages if needed
RUN pip install --upgrade pip
USER superset

# Copy your custom config
COPY ./superset/superset_config.py /etc/superset/superset_config.py

EXPOSE 8088


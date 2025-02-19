FROM python:3.10-slim

WORKDIR /app

COPY ./docker/sql_mesh/requirements.txt /app/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

COPY . /app

EXPOSE 8000

# Default command runs the web app
CMD ["sqlmesh", "web", "--host", "0.0.0.0", "--port", "8000"]


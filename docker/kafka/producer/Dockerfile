FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY ./docker/kafka/producer/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy scripts after installing dependencies (Best practice)
COPY ./kafka/scripts/ /app/scripts/

CMD ["python", "/app/scripts/kafka_producer.py"]

FROM python:3.10-slim

WORKDIR /app

# Copy and install dependencies
COPY ./docker/kafka/consumer/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy scripts after installing dependencies
COPY ./kafka/scripts/ /app/scripts/

CMD ["python", "/app/scripts/kafka_consumer.py"]

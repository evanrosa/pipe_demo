FROM python:3.10-slim

WORKDIR /app

# Copy requirements file (you can create one with kafka-python as the dependency)
COPY ./docker/kafka/topic_creator/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy scripts (adjust paths as needed)
COPY ./kafka/scripts /app/kafka/scripts
COPY ./kafka/topics_config.json /app/kafka/topics_config.json

CMD ["python", "/app/kafka/scripts/create_topics.py"]

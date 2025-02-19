FROM python:3.10-slim

WORKDIR /app

# Copy and install dependencies
COPY ./docker/sql_mesh/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy transformation files after installing dependencies
COPY ./sql_mesh/transformations /app/transformations

CMD ["sqlmesh", "run", "--target", "postgres"]

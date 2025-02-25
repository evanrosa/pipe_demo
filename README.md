# Pipe-Style Data Engineering Demo 

This repository demonstrates a **streaming and batch data pipeline** inspired by Pipe’s mission of analyzing recurring revenue data for financing. Here, I use Google BigQuery’s sandbox (free tier) for data storage and SQLMesh for transformations—plus local Kafka for streaming events.

## Overview

- **Purpose:**  
  Originally developed to demonstrate my ability to build scalable data solutions for subscription-based businesses. It simulates real-world scenarios with synthetic data over multiple years and integrates advanced data processing techniques.

- **Key Technologies:**  
  - **Kafka:** For real-time event ingestion across multiple topics (subscriptions, revenue, churn, customer engagement).  
  - **BigQuery:** As the data warehouse to showcase cloud-based data warehousing.  
  - **SQLMesh:** For SQL transformations and data modeling.  
  - **Preset(Superset):** For building interactive dashboards and visualizations.  
  - **Python & Docker:** To orchestrate and containerize the entire stack.

## Architecture

1. **Data Ingestion:**  
   - Kafka producer generates synthetic events (subscriptions, revenue, churn, engagement) with historical timestamps.  
   - Topics are created via a Kafka admin script using a JSON config.

2. **Data Processing:**  
   - Kafka consumers read events from each topic concurrently (using Python threads) and write them to BigQuery.
   - SQLMesh transforms raw data into analytics-ready tables.

3. **Visualization:**  
   - Preset connects to BigQuery for interactive dashboards.

## How to Run

1. **Pre-requisites:**
   - Docker & Docker Compose installed.
   - A `.env` file with necessary credentials and configuration.
   - Google Cloud service account key (mounted in the container) for BigQuery access.
  
2. **Spin Up the Stack:**
   ```bash
   docker-compose up --build

3. **Access Dashboards with Preset**

4. **Interacting with Pipeline**
   - Synthetic events will be ingested automatically by the Kafka producer and processed by consumers.


## Why This Project?

Even though the original job posting is no longer available, this project remains a testament to my skills in building modern data architectures, handling streaming data, and creating effective data visualizations. It demonstrates a practical, hands-on approach to solving real business problems, and is a valuable portfolio piece to show to potential employers.


### CMDs Noted:
```bash
docker compose up -d up
docker compose run --rm sqlmesh sqlmesh plan / ui / info / run
docker compose run --rm sqlmesh sqlmesh plan --restate-model db.table_name -s YYYY-MM-DD -e YYYY-MM-DD 
```
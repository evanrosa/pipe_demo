# Pipe-Style Data Engineering Demo (BigQuery Sandbox Edition)

This repository demonstrates a **streaming and batch data pipeline** inspired by Pipe’s mission of analyzing recurring revenue data for non-dilutive financing. Here, we use Google BigQuery’s sandbox (free tier) for data storage and SQLMesh for transformations—plus local Kafka for streaming events.

## Overview

1. **Streaming Ingestion (Kafka)**  
   - Synthetic events (e.g., subscription sign-ups, churn, payments) are published to Kafka in real time.  
   - Containers are orchestrated locally using Docker Compose (Kafka + Zookeeper).

2. **Cloud Data Storage (BigQuery Sandbox)**  
   - Raw events are batched from Kafka and loaded into BigQuery’s free sandbox.  
   - No local database required; the sandbox handles up to 10GB storage and 1TB of processing per month under the free tier.

3. **Transformations (SQLMesh)**  
   - Batch transformation scripts convert raw data into curated tables.  
   - SQLMesh versioning ensures reproducibility across multiple transformations.

4. **Risk & Underwriting Logic (Chalk)**  
   - Chalk can handle underwriting rules (e.g., monthly recurring revenue thresholds, churn risk).  
   - Simple integration calls Chalk’s logic engine to produce a risk or eligibility score.

5. **Forecasting Model (Baseten)**  
   - A small ML model (e.g., churn prediction) is deployed on Baseten.  
   - Any updated BigQuery data can trigger scoring requests via Baseten’s API.

6. **Dashboards (Preset)**  
   - Preset (or Superset) is used for visualization.  
   - Connects directly to BigQuery for near real-time insights on subscription trends and underwriting status.

---

## Getting Started

### 1. Clone the Repository
```bash
git clone repo
cd root

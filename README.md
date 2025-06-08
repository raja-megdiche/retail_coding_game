# Retail Data Pipeline Project

This project contains two Apache Airflow DAGs for ingesting and processing retail data. The architecture includes 2 layers : **Bronze** and **Silver** to ensure  separation between raw and transformed data (ELT).

---

## DAGs Overview

### 1. `ingestion_referential_init`

**Description:**  
Ingests referential data files for **clients**, **stores**, and **products**, and stores them into both **Bronze** (raw) and **Silver** (cleaned) layers.

- **Run Frequency:** Once
- **Purpose:** Assumes referential data is static and serves as the foundation for downstream processing.

### 2. `ingestion_transactions_daily`

**Description:**  
Uses a `WasbPrefixSensor` to wait for daily transaction files arriving in **Azure Blob Storage**. Once detected, it processes and stores the data.

- **Run Frequency:** Daily at **9:00 AM**
- **Purpose:** Automates daily ingestion of transaction data.

---

## Data Architecture

- **Bronze Layer**: Stores raw ingested data exactly as received.
- **Silver Layer**: Stores cleaned and transformed data, including constraint validations and business logic.

---

## Prerequisites

- [Docker](https://www.docker.com/) installed
- [Docker Compose](https://docs.docker.com/compose/) available

---

## Getting Started
You need to first replace the credentials in the config.py file ( connection string and container name).

### Step 1: Reset the Environment

Remove existing containers and volumes:

```bash
echo AIRFLOW_UID=1000 > .env
docker compose down --volumes
```
### Step 2: Start the Infrastructure

Remove existing containers and volumes:

```bash
docker compose up
```
This will spin up:

- **Airflow**: Handles DAG orchestration and scheduling through its webserver, scheduler, and worker components.
- **PostgreSQL**: Serves as both the Airflow metadata database and the backend for storing raw (Bronze) and processed (Silver) data.
- **Redis**: Acts as the message broker for Celery, which is used by Airflow for task execution in distributed mode.
- **pgAdmin**: A web-based interface for interacting with the PostgreSQL database, useful for inspecting table schemas and querying data ( you
can also interact directly with psql in the container to query the database).
- ## Accessing the Web Interfaces

### Airflow UI

- **URL**: [http://localhost:8080](http://localhost:8080)
- **Login**:
    - **Username**: `airflow`
    - **Password**: `airflow`

---

### pgAdmin UI

- **URL**: [http://localhost:8081](http://localhost:8081)

After logging in to pgAdmin, create a new connection using the following credentials:

- **Name**: `airflow`
- **Host**: `postgres`
- **Username**: `airflow`
- **Password**: `airflow`
- **Database**: `airflow`
- **Port**: `5432`

This gives you access to the underlying PostgreSQL database where Airflow metadata and your Bronze/Silver layer tables are stored.

---

## DAG Activation Workflow

By default, all DAGs are **paused** when Airflow starts. Follow the steps below to run your pipeline in the correct order:

### 1. Activate `ingestion_referential_init`

- In the **Airflow UI** (`http://localhost:8080`), navigate to the `ingestion_referential_init` DAG.
- Toggle the switch to **activate** it.
- Wait for the DAG to complete successfully.

Once complete:

- Use **pgAdmin** (`http://localhost:8081`) or psql on the container to inspect the following tables in the **bronze and silver layers**:
    - `clients`
    - `stores`
    - `products`
### 2. Activate `ingestion_transactions_daily`

- In the Airflow UI, activate the `ingestion_transactions_daily` DAG.
- It will automatically pick up transaction files that have already arrived in Azure Blob Storage.
- Once the DAG run completes successfully:

You can then inspect:

- The `transactions` table in the **bronze layer** (raw data)
- The `transactions` table in the **silver layer** (cleaned and validated data)

---
## Improvements :
Those are some of the points to improve since I did not have enough time to deliver a proper solution:
- The credentials are supposed to be saved in a vault tool and/or encrypted via git-crypt for example.
- Properly structure the project and add documentation and unit/IT tests
- Airflow is not used to init tables like I did. It is not typically used to one-shot use cases and is rather a scheduler/orchestration tool. 
- The architecture is supposed to have 3 layers : bronze, silver ang gold :
  - bronze: raw data as we receive it
  - silver : 
    - Remove the lines that contains important columns with null data or fill them
    - Remove data with wrong formats ( exp hours not between 0 and 24, not unique emails per client id, some dates from 1990 that I noticed in some tables etc) 
    - Choose properly the primay keys for tables ( for example both the transaction id and the timestamp of the transaction)
    - We can also create discard tables and metrics and trigger alerts if a threshold of discard is reached
    - Remove duplicate keys and chose to keep last ( upsert ) 
    - We can use the primary key starting from silver to ensure uniqueness to downstream
    - I used a classic relational DB ( for easy constraint handling), in real life for example If I chose bigquery, I need to handle all of the constraints before ingesting in the tables
  - gold : In the gold layer we will have the most de-normalized form to be consumed for analytics



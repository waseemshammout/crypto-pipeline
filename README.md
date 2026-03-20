# 🚀 Crypto: Automated ETL Pipeline
A resilient, containerized, and orchestrated data pipeline that tracks cryptocurrency prices in real-time.

## Architecture
We use a modern Data Engineering stack to move data from the Binance API to a remote Postgres warehouse.

```mermaid
graph TD
    A[Binance API] -->|JSON| B(Prefect Flow)
    subgraph Docker Container
        B --> C{Transform}
        C -->|Pandas| D[Clean Data]
    end
    D -->|SQLAlchemy| E[(Aiven Postgres)]
    F[Prefect Cloud/Local] -.->|Orchestrate| B
    G[GitHub Actions] -.->|Lint| B

```

## Dashboard Preview
![crypto-pipeline](assets/1-3.PNG)
![crypto-pipeline](assets/2-3.PNG)
![crypto-pipeline](assets/3-3.PNG)

# Tech Stack
* Language: Python 3.11
* Orchestration: Prefect (Scheduled 1min intervals)
* Storage: PostgreSQL (Hosted on aiven.io)
* Infrastructure: Docker & Docker Compose

# How to Run
## 1. Prerequisites
* Docker & Docker Compose installed.
* A free Postgres instance (e.g., Aiven).

## 2. Setup Environment
* Create a .env file in the root directory:

```bash
DATABASE_URL=postgresql://user:pass@host:port/dbname
PREFECT_API_URL=[http://host.docker.internal:4200/api](http://host.docker.internal:4200/api)

```
## 3. Launch
* Start the Prefect Server (Local Host)
```bash
prefect server start --host 0.0.0.0
```
* Build containerized pipeline
```bash
docker-compose build
```
* Run the containerized pipeline
```bash
docker-compose up -d
```
* Monitor process logs
```bash
docker logs -f crypto_etl_runner

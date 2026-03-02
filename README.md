Automated Crypto Currency Price Ingestion Engine
# 🚀 Crypto-Sentinel: Automated ETL Pipeline
A resilient, containerized, and orchestrated data pipeline that tracks cryptocurrency prices in real-time.

## 🏗️ Architecture
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
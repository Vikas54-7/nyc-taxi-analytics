\# NYC Taxi Analytics Platform



> Real-time data engineering platform processing 9.2M+ NYC TLC trip records

&#x20;

> Demonstrating production-grade streaming, analytics, and orchestration



\[!\[Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)

\[!\[Kafka](https://img.shields.io/badge/Apache-Kafka-black.svg)](https://kafka.apache.org/)

\[!\[Airflow](https://img.shields.io/badge/Apache-Airflow-red.svg)](https://airflow.apache.org/)

\[!\[DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow.svg)](https://duckdb.org/)



\---



\## 📊 Project Metrics



| Metric | Value |

|--------|-------|

| Total Trips Processed | 9,211,252 |

| Revenue Tracked | $253,375,728.66 |

| Data Volume | 160MB (Parquet) |

| Date Range | Jan-Mar 2024 |

| Query Performance | Sub-second |

| Data Quality | 99%+ |



\---



&#x20;🎯 Project Overview



Built a production-ready data engineering platform processing official NYC Taxi \& Limousine Commission (TLC) trip records. The system demonstrates end-to-end pipeline development from real-time streaming ingestion through Apache Kafka to interactive analytics visualization.



Key Features:

\- ✅ Real-time data streaming with Apache Kafka

\- ✅ Sub-second OLAP analytics with DuckDB

\- ✅ Automated workflow orchestration with Airflow

\- ✅ Data quality validation (99%+ accuracy)

\- ✅ REST API with Flask

\- ✅ Interactive dashboard

\- ✅ Containerized with Docker



\---



\## 🏗️ Architecture

```

Data Source (NYC TLC Parquet)

&#x20;       ↓

Apache Kafka (Streaming)

&#x20;       ↓

Python Consumers

&#x20;       ↓

DuckDB (Analytics Warehouse)

&#x20;       ↓

Flask REST API

&#x20;       ↓

Interactive Dashboard

```



Orchestration: Apache Airflow DAGs manage the entire ETL workflow



\---



\## 💻 Technology Stack



| Category | Technologies |

|----------|-------------|

| Streaming | Apache Kafka, Python Producers/Consumers |

| Analytics | DuckDB, SQL |

| Orchestration | Apache Airflow, DAGs |

| Processing | Python, Pandas |

| API | Flask, REST |

| Visualization | HTML/CSS/JS, Chart.js |

| Deployment | Docker, Docker Compose |

| Version Control | Git, GitHub |



\---



\## 📈 Key Achievements



\### Real-Time Data Streaming

\- Implemented Kafka pipeline processing 9.5M+ records

\- 57 messages/second throughput

\- Exactly-once processing semantics

\- Automatic topic creation and consumer group management



\### High-Performance Analytics

\- DuckDB OLAP queries execute in sub-second time

\- Complex aggregations on 9.2M trips

\- Hourly patterns, payment analysis, location rankings

\- Optimized query performance



\### Data Quality

\- Automated validation framework

\- 99%+ data quality score

\- Filters invalid records (negative fares, zero distances)

\- Anomaly detection and reporting



\### Pipeline Orchestration

\- Apache Airflow DAGs for complete ETL workflow

\- Scheduled execution with error handling

\- Health checks and monitoring

\- Retry logic for failed tasks



\---



📊 Analytics Insights



Hourly Patterns:

\- Peak hour: 6 PM (666,981 trips)

\- Lowest: 4 AM (52,827 trips)



Payment Breakdown:

\- Credit Card: 78% (7.2M trips, $204M)

\- Cash: 14% (1.3M trips, $31M)

\- Other: 8%



Performance:

\- Average fare: $27.51

\- Average distance: 4.14 miles

\- Average tip: $4.20 (credit card transactions)



\---



🚀 Quick Start



Prerequisites

\- Python 3.9+

\- Docker (optional, for Kafka)

\- 2GB free disk space



Installation

```bash

\# Clone repository

git clone https://github.com/Vikas54-7/nyc-taxi-analytics.git

cd nyc-taxi-analytics



\# Create virtual environment

python -m venv venv

source venv/bin/activate  # On Windows: venv\\Scripts\\activate



\# Install dependencies

pip install -r requirements.txt

```



\### Run Analytics

```bash

\# Process data with DuckDB

python scripts/duckdb\_analytics.py

```



\### Start API Server

```bash

\# Start Flask API

python scripts/api\_server.py



\# Access endpoints:

\# http://localhost:5000/api/summary

\# http://localhost:5000/api/hourly

\# http://localhost:5000/api/by-payment

```



\### View Dashboard

```bash

\# Start HTTP server

python -m http.server 8000



\# Open browser:

\# http://localhost:8000/scripts/dashboard.html

```



\---



\## 📁 Project Structure

```

nyc-taxi-analytics/

├── data/

│   ├── raw/                    # Original parquet files

│   └── warehouse/              # DuckDB database

├── scripts/

│   ├── duckdb\_analytics.py    # Analytics engine

│   ├── api\_server.py          # Flask API

│   └── dashboard.html         # Interactive UI

├── kafka/

│   ├── producer/              # Kafka producers

│   └── consumer/              # Kafka consumers

├── airflow/

│   └── dags/                  # Workflow DAGs

├── docker-compose.yml         # Container orchestration

└── requirements.txt           # Dependencies

```



\---



\## 🎯 Skills Demonstrated



\- Data Engineering: Pipeline design, ETL workflows, data quality

\- Streaming: Real-time processing with Apache Kafka

\- Analytics: OLAP optimization, complex SQL queries

\- Orchestration: Workflow automation with Airflow

\- API Development: RESTful design with Flask

\- Containerization: Docker deployment

\- Version Control: Git best practices





&#x20;📝 License



This project is for educational and portfolio demonstration purposes.  

Data source: NYC Taxi \& Limousine Commission (TLC) - Public Dataset



&#x20; 

https://www.linkedin.com/in/pabbavikas/ | \[GitHub](https://github.com/Vikas54-7)


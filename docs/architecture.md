\# NYC Taxi Analytics Platform - Architecture



\## System Architecture Diagram

```

┌─────────────────────────────────────────────────────────────────────────────┐

│                        NYC TAXI ANALYTICS PLATFORM                          │

├─────────────────────────────────────────────────────────────────────────────┤

│                                                                             │

│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │

│  │   NYC TLC   │    │   APACHE    │    │   PYTHON    │    │   DUCKDB    │  │

│  │   PARQUET   │───▶│   KAFKA     │───▶│  CONSUMER   │───▶│  WAREHOUSE  │  │

│  │   FILES     │    │             │    │             │    │             │  │

│  │  (9.5M rows)│    │ taxi-trips  │    │ kafka\_to\_   │    │ raw\_trips   │  │

│  └─────────────┘    │   topic     │    │ duckdb.py   │    │   table     │  │

│        │            └─────────────┘    └─────────────┘    └──────┬──────┘  │

│        │                   │                                      │        │

│        ▼                   ▼                                      ▼        │

│  ┌─────────────┐    ┌─────────────┐                        ┌─────────────┐ │

│  │   PYTHON    │    │  KAFKA UI   │                        │  ANALYTICS  │ │

│  │  PRODUCER   │    │  (Monitor)  │                        │   SCRIPTS   │ │

│  │             │    │             │                        │             │ │

│  │ taxi\_       │    │ localhost:  │                        │ analytics.  │ │

│  │ producer.py │    │    8080     │                        │    py       │ │

│  └─────────────┘    └─────────────┘                        └──────┬──────┘ │

│                                                                    │        │

│  ┌─────────────────────────────────────────────────────────────────┼──────┐ │

│  │                      APACHE AIRFLOW                             │      │ │

│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │      │ │

│  │  │  CHECK   │  │ PRODUCE  │  │ CONSUME  │  │ QUALITY  │        │      │ │

│  │  │  KAFKA   │─▶│   DATA   │─▶│   DATA   │─▶│  CHECK   │────────┘      │ │

│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘               │ │

│  │                                                     │                  │ │

│  │  Scheduler: Daily @ 6:00 AM                         ▼                  │ │

│  │  UI: localhost:8085                          ┌──────────┐              │ │

│  │                                              │ GENERATE │              │ │

│  │                                              │ REPORTS  │              │ │

│  └──────────────────────────────────────────────┴──────────┴──────────────┘ │

│                                                        │                    │

│                                                        ▼                    │

│                                                 ┌─────────────┐             │

│                                                 │  DASHBOARD  │             │

│                                                 │   (HTML)    │             │

│                                                 │             │             │

│                                                 │ index.html  │             │

│                                                 └─────────────┘             │

│                                                                             │

└─────────────────────────────────────────────────────────────────────────────┘

```



\## Data Flow



1\. \*\*Data Source\*\*: NYC TLC official Parquet files (9.5M+ taxi trip records)

2\. \*\*Ingestion\*\*: Python Kafka Producer streams data to Kafka topic

3\. \*\*Message Queue\*\*: Apache Kafka buffers and distributes messages

4\. \*\*Processing\*\*: Python Kafka Consumer processes and validates data

5\. \*\*Storage\*\*: DuckDB analytical warehouse stores processed data

6\. \*\*Analytics\*\*: Python scripts generate insights and reports

7\. \*\*Visualization\*\*: HTML dashboard displays KPIs and charts

8\. \*\*Orchestration\*\*: Apache Airflow schedules and monitors pipeline



\## Tech Stack



| Layer | Technology | Purpose |

|-------|------------|---------|

| Data Source | NYC TLC Parquet | Official taxi trip records |

| Message Queue | Apache Kafka | Real-time data streaming |

| Processing | Python + Pandas | Data transformation |

| Warehouse | DuckDB | Analytical queries |

| Orchestration | Apache Airflow | Pipeline scheduling |

| Monitoring | Kafka UI | Stream monitoring |

| Dashboard | HTML/CSS/JS | Data visualization |

| Containers | Docker | Service deployment |



\## Services \& Ports



| Service | Port | URL |

|---------|------|-----|

| Kafka | 9092 | localhost:9092 |

| Kafka UI | 8080 | http://localhost:8080 |

| Airflow | 8085 | http://localhost:8085 |

| PostgreSQL | 5432 | localhost:5432 |

| Zookeeper | 2181 | localhost:2181 |



\## Key Metrics



| Metric | Value |

|--------|-------|

| Total Records Available | 9,554,778 |

| Records Processed | 86,980 |

| Total Revenue | $2,662,635 |

| Avg Processing Rate | 57 msg/sec |

| Data Quality Score | 99%+ |



\## Pipeline Schedule



\- \*\*Daily Pipeline\*\*: Runs at 6:00 AM UTC

\- \*\*Tasks\*\*: Kafka check → Produce → Consume → Quality Check → Report



\## Scaling Considerations



For production deployment:

\- Use Apache Spark for large-scale processing

\- Deploy on GCP Dataproc or AWS EMR

\- Replace DuckDB with BigQuery or Snowflake

\- Add Prometheus/Grafana for monitoring

\- Implement CI/CD with GitHub Actions


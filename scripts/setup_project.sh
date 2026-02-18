#!/bin/bash

# NYC Taxi Analytics Platform - Initial Setup Script
# Author: Vikas Pabba

set -e  # Exit on error

echo "=========================================="
echo "NYC Taxi Analytics Platform Setup"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if running from project root
if [ ! -f "requirements.txt" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Create directory structure
print_status "Creating project directory structure..."

directories=(
    "kafka/producer"
    "kafka/config"
    "spark/streaming"
    "spark/batch"
    "spark/utils"
    "airflow/dags"
    "airflow/plugins"
    "druid/ingestion_specs"
    "druid/queries"
    "trino/catalog"
    "trino/queries"
    "api/src/main/java/com/vikas/taxianalytics"
    "api/src/main/resources"
    "kubernetes"
    "data_quality/expectations"
    "monitoring/prometheus"
    "monitoring/grafana/dashboards"
    "notebooks"
    "scripts"
    "terraform/gcp"
    "docs"
    "tests/unit"
    "tests/integration"
    "logs"
    "data/raw"
    "data/processed"
)

for dir in "${directories[@]}"; do
    mkdir -p "$dir"
done

print_status "Directory structure created successfully!"

# Create placeholder files
print_status "Creating placeholder files..."

# Create __init__.py files for Python packages
find . -type d -name "spark" -o -name "kafka" -o -name "airflow" -o -name "data_quality" | while read dir; do
    find "$dir" -type d -exec touch {}/__init__.py \;
done

# Create .env.example
cat > .env.example << 'EOF'
# GCP Configuration
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
GCP_ZONE=us-central1-a

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TAXI_TRIPS=taxi-trips

# Spark Configuration
SPARK_MASTER=yarn
SPARK_APP_NAME=nyc-taxi-analytics

# BigQuery Configuration
BQ_DATASET=taxi_analytics
BQ_TABLE=trips

# Druid Configuration
DRUID_HOST=localhost
DRUID_PORT=8888

# Trino Configuration
TRINO_HOST=localhost
TRINO_PORT=8080

# API Configuration
API_PORT=8080
API_HOST=0.0.0.0

# Airflow Configuration
AIRFLOW_HOME=~/airflow
AIRFLOW__CORE__DAGS_FOLDER=./airflow/dags

# Monitoring
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
EOF

print_status ".env.example created!"

# Create docker-compose.yml for local development
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data

  postgres:
    image: postgres:15
    container_name: postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus/config.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
  postgres-data:
  prometheus-data:
  grafana-data:

networks:
  default:
    name: taxi-analytics-network
EOF

print_status "docker-compose.yml created!"

# Create basic Prometheus config
mkdir -p monitoring/prometheus
cat > monitoring/prometheus/config.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']

  - job_name: 'spark'
    static_configs:
      - targets: ['localhost:4040']
EOF

print_status "Prometheus configuration created!"

# Create LICENSE
cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2025 Vikas Pabba

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF

print_status "LICENSE created!"

# Make scripts executable
chmod +x scripts/*.sh 2>/dev/null || true

echo ""
echo "=========================================="
print_status "Setup completed successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Copy .env.example to .env and update with your credentials"
echo "   cp .env.example .env"
echo ""
echo "2. Create and activate Python virtual environment:"
echo "   python -m venv venv"
echo "   source venv/bin/activate  # On Windows: venv\\Scripts\\activate"
echo ""
echo "3. Install dependencies:"
echo "   pip install -r requirements.txt"
echo ""
echo "4. Start local services:"
echo "   docker-compose up -d"
echo ""
echo "5. Initialize Git repository (if not already done):"
echo "   git init"
echo "   git add ."
echo "   git commit -m 'Initial project setup'"
echo "   git remote add origin https://github.com/Vikas54-7/nyc-taxi-analytics.git"
echo "   git push -u origin main"
echo ""
print_warning "Don't forget to update .env with your actual credentials!"
echo ""
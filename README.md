# Mastodon Real-Time Analytics Pipeline

A Big Data pipeline that collects public posts from the Mastodon social network in real time, processes them with Apache Flink, stores the results in HDFS and Elasticsearch, and visualizes analytics through Kibana dashboards.

---

## Table of Contents

- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Details](#pipeline-details)
- [Documentation](#documentation)

---

## Architecture

```
Mastodon API
     |
     | (polling hashtag #news every 3s)
     v
test_pipelines.py
     |  MastodonClient -> StreamHandler -> KafkaProducer
     v
Apache Kafka [Mastodon_stream]
     |
     +---> consumer_main.py ---------> Apache HDFS
     |       (raw post storage)         hdfs://localhost:9000/kafka_demo/tweets_data.json
     |
     +---> run_flink_analysis.py -----> Apache Flink (PyFlink)
              |                           |
              |  PostParser               +---> EngagementMapper ---> Kafka [mastodon-engagement]
              |                           |
              |                           +---> HashtagExtractor ---> Kafka [mastodon-hashtags]
              |                                       |
              v                                       v
         es_consumer.py ---------> Elasticsearch (indexing)
                                         |
                                         v
                                      Kibana (visualization)
                                      http://localhost:5601
```

---

## Technologies

| Component        | Technology              | Version  | Purpose                          |
|------------------|-------------------------|----------|----------------------------------|
| Data Source       | Mastodon API            | -        | Public post collection           |
| Message Broker    | Apache Kafka (KRaft)    | 4.1.1    | Real-time message queue          |
| Stream Processing | Apache Flink (PyFlink)  | 2.2.0    | Real-time data analysis          |
| Batch Storage     | Apache Hadoop HDFS      | -        | Raw data persistence             |
| Search Engine     | Elasticsearch           | 8.17.4   | Indexed document storage         |
| Visualization     | Kibana                  | 8.17.4   | Dashboards and analytics         |
| Language          | Python                  | >= 3.11  | Application code                 |
| Package Manager   | uv                      | -        | Dependency management            |

---

## Project Structure

```
Big-Data-Project/
├── pyproject.toml                        # Project dependencies and metadata
├── README.md
├── .env                                  # Mastodon API credentials (not tracked)
├── .env.example                          # Template for .env
│
├── src/
│   └── Mastodon_stream/
│       ├── producer/                     # Mastodon -> Kafka
│       │   ├── mastodon_client.py        # Mastodon API polling client
│       │   ├── stream_handler.py         # Post processing and Kafka dispatch
│       │   └── kafka_producer.py         # Kafka producer wrapper
│       │
│       ├── consumer/                     # Kafka -> Storage
│       │   ├── kafka_consumer.py         # Generic Kafka consumer
│       │   ├── hdfs_writer.py            # HDFS file writer (pydoop)
│       │   └── elasticsearch_writer.py   # Elasticsearch indexer
│       │
│       └── flink_analysis/               # Kafka -> Flink -> Kafka
│           ├── stream_analyzer.py        # DataStream API (MapFunctions)
│           └── sql_analyzer.py           # Table/SQL API (windowed queries)
│
├── tests/
│   ├── test_pipelines.py                 # Entry point: Mastodon -> Kafka
│   ├── consumer_main.py                  # Entry point: Kafka -> HDFS
│   ├── run_flink_analysis.py             # Entry point: Flink analysis
│   └── es_consumer.py                    # Entry point: Kafka -> Elasticsearch
│
└── docs/
    ├── Kafka_installation.md
    ├── Flink_installation.md
    ├── Elasticsearch_installation.md
    └── Kibana_installation.md
```

---

## Prerequisites

- Ubuntu 20.04 or later
- Java 8 (OpenJDK) for Hadoop
- Java 17 (OpenJDK) for Kafka, Flink, and Elasticsearch
- Python 3.11 or later
- uv package manager

### External services (installed separately)

| Service        | Location                                    | Port  |
|----------------|---------------------------------------------|-------|
| Hadoop HDFS    | System default                              | 9000  |
| Kafka          | ~/Downloads/kafka_2.13-4.1.1                | 9092  |
| Flink          | ~/Downloads/flink-2.2.0                     | 8081  |
| Elasticsearch  | ~/Downloads/elasticsearch-8.17.4            | 9200  |
| Kibana         | ~/Downloads/kibana-8.17.4                   | 5601  |

---

## Installation

### 1. Clone the repository
```bash
git clone <repository-url>
cd Big-Data-Project
```

### 2. Install Python dependencies
```bash
uv sync
```

### 3. Install external services

Follow the guides in the `docs/` folder:

1. [Kafka Installation](docs/Kafka_installation.md) -- Message broker (KRaft mode, no Zookeeper)
2. [Flink Installation](docs/Flink_installation.md) -- Stream processing engine
3. [Elasticsearch Installation](docs/Elasticsearch_installation.md) -- Search and indexing
4. [Kibana Installation](docs/Kibana_installation.md) -- Visualization dashboards

---

## Configuration

### Mastodon API credentials

Create a `.env` file at the project root:
```bash
cp .env.example .env
```

Fill in the credentials:
```
CLIENT_KEY=your_client_key
CLIENT_SECRET=your_client_secret
ACCESS_TOKEN=your_access_token
```

To obtain credentials:
1. Go to https://mastodon.social/settings/applications
2. Create a new application with scopes: `read`, `read:statuses`, `profile`
3. Copy the generated keys into `.env`

---

## Usage

### Start services (Phase 1)

Start each service in a separate terminal, in this order:

```bash
# Terminal 1 -- HDFS
start-dfs.sh

# Terminal 2 -- Kafka
cd ~/Downloads/kafka_2.13-4.1.1 && ./bin/kafka-server-start.sh config/server.properties

# Terminal 3 -- Elasticsearch
nohup ~/Downloads/elasticsearch-8.17.4/bin/elasticsearch > /tmp/es.log 2>&1 &

# Terminal 4 -- Kibana
nohup ~/Downloads/kibana-8.17.4/bin/kibana > /tmp/kibana.log 2>&1 &
```

### Run the pipeline (Phase 2)

```bash
# Terminal 5 -- Producer: Mastodon -> Kafka
uv run python -m tests.test_pipelines

# Terminal 6 -- Flink analysis: Kafka -> process -> Kafka
uv run python -m tests.run_flink_analysis --mode stream --analysis full

# Terminal 7 -- Elasticsearch consumer: Kafka -> Elasticsearch
uv run python -m tests.es_consumer

# Terminal 8 -- HDFS consumer: Kafka -> HDFS
uv run python -m tests.consumer_main
```

### View results

- Kibana dashboards: http://localhost:5601
- Elasticsearch raw queries:
  ```bash
  curl -s "http://localhost:9200/mastodon-engagement/_count"
  curl -s "http://localhost:9200/mastodon-hashtags/_count"
  curl -s "http://localhost:9200/mastodon-engagement/_search?pretty&size=5"
  ```

### Stop everything (reverse order)

```bash
# Stop Python scripts: Ctrl+C in each terminal
# Stop Kibana
pkill -f kibana
# Stop Elasticsearch
pkill -f elasticsearch
# Stop Kafka: Ctrl+C or
~/Downloads/kafka_2.13-4.1.1/bin/kafka-server-stop.sh
# Stop HDFS
stop-dfs.sh
```

---

## Pipeline Details

### Producer (test_pipelines.py)

Connects to the Mastodon API via polling on the `#news` hashtag. Every 3 seconds, fetches new posts and sends them to the Kafka topic `Mastodon_stream`. Each post is transformed into a JSON object containing: id, content, account info, language, engagement counts, and tags.

**Modules used:** `MastodonClient`, `StreamHandler`, `KafkaProducer`

### Flink Analysis (run_flink_analysis.py)

Reads from `Mastodon_stream`, applies transformations via PyFlink, and writes to output topics.

**DataStream mode** (`--mode stream`):

| Analysis    | Description                                    | Output Topic         |
|-------------|------------------------------------------------|----------------------|
| engagement  | Adds engagement_score to each post             | mastodon-engagement  |
| hashtags    | Extracts hashtags with counts                  | mastodon-hashtags    |
| language    | Filters posts by language                      | mastodon-filtered    |
| full        | Runs engagement + hashtags in parallel         | Both topics          |

**SQL mode** (`--mode sql`):

| Analysis    | Description                                    | Output               |
|-------------|------------------------------------------------|-----------------------|
| engagement  | Windowed engagement metrics (avg, max)         | Terminal (print sink) |
| language    | Language distribution per window               | Terminal (print sink) |
| users       | Most active users per window                   | Terminal (print sink) |
| count       | Simple post count per 10-second window         | mastodon-counts topic |
| dashboard   | Combined metrics per minute                    | mastodon-dashboard    |

### HDFS Consumer (consumer_main.py)

Reads raw posts from `Mastodon_stream` and appends them as JSON lines to HDFS at `hdfs://localhost:9000/kafka_demo/tweets_data.json`. Intended for batch processing with Spark or MapReduce.

**Modules used:** `MastodonKafkaConsumer`, `HDFSWriter`

### Elasticsearch Consumer (es_consumer.py)

Reads analyzed data from Flink output topics (`mastodon-engagement`, `mastodon-hashtags`) and indexes them into Elasticsearch. Uses post ID as document ID to prevent duplicates. Each document receives an `indexed_at` timestamp.

**Modules used:** `ElasticsearchWriter`, `confluent_kafka.Consumer`

### Kibana

Connects to Elasticsearch on port 9200. Provides visualization through Data Views mapped to the two indices: `mastodon-engagement` and `mastodon-hashtags`. Supports dashboards with language distribution, post volume over time, top hashtags, and engagement metrics.

---

## Kafka Topics

| Topic                | Producer              | Consumer             | Content                   |
|----------------------|-----------------------|----------------------|---------------------------|
| Mastodon_stream      | test_pipelines.py     | Flink, consumer_main | Raw Mastodon posts        |
| mastodon-engagement  | Flink                 | es_consumer.py       | Posts with engagement score|
| mastodon-hashtags    | Flink                 | es_consumer.py       | Extracted hashtags        |
| mastodon-counts      | Flink (SQL mode)      | Console consumer     | Post counts per window    |
| mastodon-dashboard   | Flink (SQL mode)      | Console consumer     | Dashboard metrics         |

---

## Documentation

Detailed installation and configuration guides are available in the `docs/` folder:

- [Kafka Installation](docs/Kafka_installation.md)
- [Flink Installation](docs/Flink_installation.md)
- [Elasticsearch Installation](docs/Elasticsearch_installation.md)
- [Kibana Installation](docs/Kibana_installation.md)

---

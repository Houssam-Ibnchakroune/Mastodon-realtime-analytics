mastodon-realtime-analytics/
├── pyproject.toml
├── uv.lock
├── .gitignore
├── . env. example
├── README.md
├── docker-compose.yml
├── Makefile
│
├── src/
│   └── mastodon_stream/
│       ├── __init__.py
│       ├── config.py
│       │
│       ├── producer/                    # Mastodon → Kafka
│       │   ├── __init__.py
│       │   ├── mastodon_client.py       # Connexion Mastodon API
│       │   ├── kafka_producer.py        # confluent-kafka producer
│       │   └── stream_handler.py        # Main producer script
│       │
│       ├── flink_jobs/                  # Flink Stream Processing
│       │   ├── __init__.py
│       │   ├── sentiment_analysis.py    # Job 1: Analyse sentiment
│       │   ├── trending_topics.py       # Job 2: Trending hashtags
│       │   ├── language_stats.py        # Job 3: Stats par langue
│       │   └── sink_elasticsearch.py    # Sink vers Elasticsearch
│       │
│       ├── hdfs/                        # HDFS Integration
│       │   ├── __init__.py
│       │   ├── kafka_to_hdfs.py         # Consumer Kafka → HDFS
│       │   └── batch_writer.py          # Écriture par batch
│       │
│       └── utils/
│           ├── __init__. py
│           ├── logging_setup.py
│           └── data_models.py           # Pydantic schemas
│
├── flink/
│   ├── conf/
│   │   └── flink-conf.yaml              # Configuration Flink
│   ├── lib/                             # Dépendances JAR
│   │   ├── flink-connector-kafka-*. jar
│   │   └── flink-connector-elasticsearch-*.jar
│   └── jobs/                            # Compiled jobs
│
├── elasticsearch/
│   ├── mappings/
│   │   ├── toots_index.json             # Mapping pour les toots
│   │   └── trends_index.json            # Mapping pour trends
│   └── data/
│
├── kibana/
│   ├── dashboards/
│   │   ├── 01-overview.ndjson           # Dashboard principal
│   │   ├── 02-sentiment-analysis.ndjson
│   │   ├── 03-trending-topics.ndjson
│   │   └── 04-language-stats.ndjson
│   └── saved_objects/
│
├── kafka/
│   └── data/                            # Kafka storage
│
├── scripts/
│   ├── setup_kafka_topics.sh            # Créer les topics Kafka
│   ├── setup_elasticsearch_indices.sh   # Créer les indices ES
│   ├── setup_hdfs. sh                    # Initialiser HDFS
│   ├── import_kibana_dashboards.sh      # Importer dashboards
│   ├── start_producer.sh                # Démarrer le producer
│   ├── submit_flink_jobs.sh             # Soumettre jobs Flink
│   └── test_pipeline.py                 # Test end-to-end
│
├── tests/
│   ├── test_producer.py
│   ├── test_flink_jobs.py
│   ├── test_hdfs_writer.py
│   └── fixtures/
│       └── sample_toots.json
│
├── docs/
│   ├── 01-architecture.md               # Architecture détaillée
│   ├── 02-installation.md               # Guide d'installation
│   ├── 03-mastodon-setup.md             # Setup Mastodon API
│   ├── 04-kafka-setup.md                # Configuration Kafka
│   ├── 05-flink-jobs.md                 # Explication des jobs
│   ├── 06-hdfs-setup.md                 # Configuration HDFS
│   ├── 07-elasticsearch. md              # Setup Elasticsearch
│   ├── 08-kibana-dashboards.md          # Dashboards Kibana
│   └── 09-troubleshooting.md
│
└── .github/
    └── workflows/
        └── ci.yml
# Guide d'Installation et Configuration : Elasticsearch 8.17.4

Ce document recapitule la procedure pour installer Elasticsearch 8.17.4 sur Ubuntu pour stocker les resultats d'analyse Flink du pipeline Mastodon.

---

## 1. Telechargement et Extraction

### Telechargement de la distribution
```bash
cd ~/Downloads
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.17.4-linux-x86_64.tar.gz
```

### Extraction de l'archive
```bash
tar -xzf elasticsearch-8.17.4-linux-x86_64.tar.gz
```

Le repertoire d'installation est : `~/Downloads/elasticsearch-8.17.4/`

Note : Elasticsearch inclut son propre JDK (Java 23). Il ignore la variable `JAVA_HOME` du systeme et utilise toujours son JDK embarque. Il n'y a donc aucun conflit avec Java 8 (Hadoop) ou Java 17 (Kafka/Flink).

---

## 2. Configuration

### Desactivation de la securite (developpement local)

Pour simplifier l'utilisation en developpement local, nous desactivons l'authentification et le SSL.

Fichier modifie : `config/elasticsearch.yml`

Lignes ajoutees a la fin du fichier :
```yaml
xpack.security.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false
```

### Reduction de la memoire JVM

La machine virtuelle dispose de 8 Go de RAM partages entre Kafka, Flink, Hadoop et Elasticsearch. Nous reduisons le heap d'Elasticsearch a 512 Mo.

Fichier cree : `config/jvm.options.d/custom.options`

Contenu :
```
-Xms512m
-Xmx512m
```

---

## 3. Lancement du Serveur

### Demarrage en arriere-plan
```bash
nohup ~/Downloads/elasticsearch-8.17.4/bin/elasticsearch > /tmp/es.log 2>&1 &
```

### Demarrage en premier plan (pour debug)
```bash
~/Downloads/elasticsearch-8.17.4/bin/elasticsearch
```

### Verification du serveur
```bash
curl -s http://localhost:9200
```

Reponse attendue :
```json
{
  "name" : "ubuntu",
  "cluster_name" : "elasticsearch",
  "version" : {
    "number" : "8.17.4"
  },
  "tagline" : "You Know, for Search"
}
```

### Verification de la sante du cluster
```bash
curl -s http://localhost:9200/_cluster/health?pretty
```

---

## 4. Client Python

### Installation
```bash
uv add "elasticsearch>=8.0.0,<9.0.0"
```

Note : Le client Python v9 n'est pas compatible avec le serveur Elasticsearch 8.x. Il faut utiliser la version 8 du client (`elasticsearch==8.19.3`).

### Module ElasticsearchWriter

Fichier : `src/Mastodon_stream/consumer/elasticsearch_writer.py`

Ce module gere la connexion a Elasticsearch et l'indexation des resultats d'analyse Flink.

---

## 5. Index Elasticsearch

### Index crees automatiquement

Le module `ElasticsearchWriter` cree deux index au demarrage :

#### mastodon-engagement
Stocke les posts enrichis avec un score d'engagement.

Champs principaux :
| Champ | Type | Description |
|-------|------|-------------|
| id | keyword | ID unique du post Mastodon |
| content | text | Contenu HTML du post |
| language | keyword | Langue detectee |
| username | keyword | Auteur du post |
| followers_count | integer | Nombre de followers |
| favourites_count | integer | Nombre de favoris |
| reblogs_count | integer | Nombre de partages |
| engagement_score | integer | Score calcule par Flink |
| tags | keyword (array) | Hashtags du post |
| created_at | date | Date de creation du post |
| indexed_at | date | Date d'indexation dans ES |

#### mastodon-hashtags
Stocke les hashtags extraits par Flink.

Champs principaux :
| Champ | Type | Description |
|-------|------|-------------|
| post_id | keyword | ID du post source |
| hashtags | keyword (array) | Liste des hashtags |
| hashtag_count | integer | Nombre de hashtags |
| language | keyword | Langue du post |
| indexed_at | date | Date d'indexation dans ES |

---

## 6. Utilisation

### Lancer le consumer Flink vers Elasticsearch
```bash
uv run python -m tests.es_consumer
```

### Options du consumer
```bash
# Consommer uniquement les posts avec engagement
uv run python -m tests.es_consumer --topics engagement

# Consommer uniquement les hashtags
uv run python -m tests.es_consumer --topics hashtags

# Les deux (par defaut)
uv run python -m tests.es_consumer --topics engagement hashtags
```

### Pipeline complet (5 terminaux)

| Terminal | Commande |
|----------|----------|
| 1 | Kafka : `~/Downloads/kafka_2.13-4.1.1/bin/kafka-server-start.sh config/server.properties` |
| 2 | Producteur : `uv run python -m tests.test_pipelines` |
| 3 | Flink : `uv run python -m tests.run_flink_analysis --mode stream --analysis full` |
| 4 | ES Consumer : `uv run python -m tests.es_consumer` |
| 5 | Elasticsearch : demarre via `nohup` (voir section 3) |

---

## 7. Commandes de Verification

### Compter les documents indexes
```bash
curl -s "http://localhost:9200/mastodon-engagement/_count"
curl -s "http://localhost:9200/mastodon-hashtags/_count"
```

### Rechercher des documents
```bash
curl -s "http://localhost:9200/mastodon-engagement/_search?pretty&size=5"
curl -s "http://localhost:9200/mastodon-hashtags/_search?pretty&size=5"
```

### Lister tous les index
```bash
curl -s "http://localhost:9200/_cat/indices?v"
```

### Supprimer un index (reinitialisation)
```bash
curl -X DELETE "http://localhost:9200/mastodon-engagement"
curl -X DELETE "http://localhost:9200/mastodon-hashtags"
```

---

## Resume Technique

| Composant | Valeur |
|-----------|--------|
| **Distribution** | Elasticsearch 8.17.4 (tar.gz Linux x86_64) |
| **JDK** | Embarque (OpenJDK 23) |
| **Java Systeme/Hadoop** | OpenJDK 8 (Inchange) |
| **Heap JVM** | 512 Mo (-Xms512m -Xmx512m) |
| **Securite** | Desactivee (dev local) |
| **Port HTTP** | 9200 |
| **Client Python** | elasticsearch 8.19.3 |
| **Index** | mastodon-engagement, mastodon-hashtags |

---

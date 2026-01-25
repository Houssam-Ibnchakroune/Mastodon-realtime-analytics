# Guide d'Installation et Configuration : Apache Flink 2.2.0

Ce document récapitule la procédure pour installer Flink 2.2.0 sur Ubuntu tout en gérant la cohabitation avec un environnement Hadoop (Java 8).

---

## 1. Résolution du Conflit Java (Multi-Version)

Flink 2.2.0 nécessite **Java 11+** (Java 17 recommandé). Hadoop nécessite **Java 8**. Nous avons isolé Flink pour qu'il soit le seul à utiliser la version récente.

### Configuration de Flink pour l'isolation Java
Nous avons injecté le chemin de Java 17 directement dans le script de configuration de Flink.

Fichier modifié : `bin/config.sh`

Lignes ajoutées au début (après le shebang) :
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Note : Cela permet à Hadoop de continuer à utiliser Java 8 par défaut sur le système.

---

## 2. Lancement du Cluster Flink

### Démarrage du cluster
```bash
cd ~/Downloads/flink-2.2.0
./bin/start-cluster.sh
```

Sortie attendue :
```
Starting cluster.
Starting standalonesession daemon on host ...
Starting taskexecutor daemon on host ...
```

### Vérification via l'interface Web
Ouvrir dans le navigateur :
```
http://localhost:8081
```

### Arrêt du cluster
```bash
./bin/stop-cluster.sh
```

---

## 3. Test avec un Job Exemple

### Exécution du WordCount
```bash
./bin/flink run examples/streaming/WordCount.jar
```

### Vérification des processus Java
```bash
jps
```

Vous devriez voir :
- `StandaloneSessionClusterEntrypoint`
- `TaskManagerRunner`

---

## 4. Configuration Kafka Connector (pour PyFlink)

### Téléchargement du connecteur Kafka
```bash
cd ~/Downloads/flink-2.2.0/lib
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
```

### Installation de PyFlink
```bash
uv add apache-flink
```

---


---

## Résumé Technique

| Composant | Valeur |
|-----------|--------|
| **Distribution** | Flink 2.2.0 (Binary Scala 2.12) |
| **Mode** | Standalone Cluster |
| **Java Flink** | OpenJDK 17 |
| **Java Système/Hadoop** | OpenJDK 8 (Inchangé) |
| **Interface Web** | http://localhost:8081 |
| **PyFlink** | apache-flink 2.2.0 |

---
# Guide d'Installation et Configuration : Apache Kafka 4.1.1 (KRaft Mode)

Ce document récapitule la procédure pour installer Kafka 4.1.1 sur Ubuntu tout en gérant la cohabitation avec un environnement Hadoop (Java 8).

---

## 1. Résolution du Conflit Java (Multi-Version)

Kafka 4.1.1 nécessite **Java 17** (version de classe 61.0). Hadoop nécessite **Java 8**. Nous avons isolé Kafka pour qu'il soit le seul à utiliser la version récente.

### Installation de Java 17
```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

### Modification de Kafka pour l'isolation
Nous avons injecté le chemin de Java 17 directement dans le script de lancement de Kafka.
Fichier modifié : bin/kafka-run-class.sh
Lignes ajoutées au début (après le shebang) :
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Note : Cela permet à Hadoop de continuer à utiliser Java 8 par défaut sur le système.

## 2. Configuration du Cluster sans Zookeeper (KRaft)
Les versions 4.x de Kafka n'utilisent plus Zookeeper. Le serveur gère lui-même ses métadonnées via le mode KRaft.

### Étape A : Génération de l'UUID
Générez un identifiant unique pour lier le stockage au cluster :
```bash
./bin/kafka-storage.sh random-uuid
# Exemple généré : PMN-6tHxRoeX0HBYeERtGw
```

### Étape B : Formatage du stockage
Le formatage remplace l'ancienne initialisation de Zookeeper. Le flag --standalone est indispensable pour un nœud unique en version 4.1+.
```bash
./bin/kafka-storage.sh format --standalone -t <VOTRE_UUID> -c config/server.properties
```

## 3. Utilisation et Commandes de Test

### Lancement du serveur
Plus besoin de lancer Zookeeper. On lance uniquement Kafka :
```bash
./bin/kafka-server-start.sh config/server.properties
```

### Création d'un Topic
```bash
./bin/kafka-topics.sh --create --topic mon-topic --bootstrap-server localhost:9092
```

### Test du flux de données
L'option --zookeeper est désormais remplacée par --bootstrap-server.

**Producer (Envoi) :**
```bash
./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic mon-topic
```

**Consumer (Réception) :**
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mon-topic --from-beginning
```

##  Résumé Technique
- **Distribution** : Kafka 4.1.1 (Version Binary)
- **Mode** : KRaft (Standalone)
- **Java Kafka** : OpenJDK 17
- **Java Système/Hadoop** : OpenJDK 8 (Inchangé)

---

Souhaitez-vous que je vous aide à configurer un **Connecteur Kafka** pour envoyer automatiquement des données vers votre **HDFS** ?

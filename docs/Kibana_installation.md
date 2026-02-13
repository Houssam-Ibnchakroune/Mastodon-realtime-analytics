# Guide d'Installation et Configuration : Kibana 8.17.4

Ce document recapitule la procedure pour installer Kibana 8.17.4 sur Ubuntu pour visualiser les donnees stockees dans Elasticsearch.

---

## 1. Telechargement et Extraction

### Telechargement de la distribution
```bash
cd ~/Downloads
wget https://artifacts.elastic.co/downloads/kibana/kibana-8.17.4-linux-x86_64.tar.gz
```

### Extraction de l'archive
```bash
tar -xzf kibana-8.17.4-linux-x86_64.tar.gz
```

Le repertoire d'installation est : `~/Downloads/kibana-8.17.4/`

Note : Kibana inclut son propre Node.js embarque. Aucune installation supplementaire n'est necessaire.

---

## 2. Connexion avec Elasticsearch

### Configuration de la connexion

Fichier modifie : `config/kibana.yml`

Par defaut, la ligne de connexion a Elasticsearch est commentee. Il faut la decommenter.

Avant :
```yaml
#elasticsearch.hosts: ["http://localhost:9200"]
```

Apres :
```yaml
elasticsearch.hosts: ["http://localhost:9200"]
```

Commande pour decommenter automatiquement :
```bash
sed -i 's/#elasticsearch.hosts: \["http:\/\/localhost:9200"\]/elasticsearch.hosts: ["http:\/\/localhost:9200"]/' ~/Downloads/kibana-8.17.4/config/kibana.yml
```

### Verification de la configuration
```bash
grep "elasticsearch.hosts" ~/Downloads/kibana-8.17.4/config/kibana.yml
```

Resultat attendu :
```
elasticsearch.hosts: ["http://localhost:9200"]
```

Note : La securite est desactivee sur Elasticsearch (voir le guide Elasticsearch_installation.md). Aucune configuration d'authentification n'est donc necessaire dans Kibana.

---

## 3. Prerequis

Elasticsearch doit etre demarre **avant** Kibana. Verifier avec :
```bash
curl -s http://localhost:9200
```

Si Elasticsearch n'est pas lance :
```bash
nohup ~/Downloads/elasticsearch-8.17.4/bin/elasticsearch > /tmp/es.log 2>&1 &
```

Attendre environ 30 secondes que le serveur soit pret.

---

## 4. Lancement de Kibana

### Demarrage en arriere-plan
```bash
nohup ~/Downloads/kibana-8.17.4/bin/kibana > /tmp/kibana.log 2>&1 &
```

### Demarrage en premier plan (pour debug)
```bash
~/Downloads/kibana-8.17.4/bin/kibana
```

### Verification du serveur

Kibana met environ 30 secondes a demarrer. Verifier avec :
```bash
curl -s http://localhost:5601/api/status | grep -o '"level":"[^"]*"'
```

Resultat attendu :
```
"level":"available"
```

### Interface Web

Ouvrir dans le navigateur :
```
http://localhost:5601
```

---

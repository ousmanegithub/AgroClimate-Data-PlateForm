# Module temps reel Kafka

Ce dossier ajoute un pipeline streaming complet pour la plateforme:

- Source temps reel: API capteur (`/sensor`)
- Source temps reel complementaire: application fichier (`field_app`) qui append un evenement toutes les 30s dans `data/field_events.ndjson`
- Ingestion streaming: notebook Marimo `api_producer.py` vers Kafka (`sensor.raw`)
- Detection d'anomalies en temps reel: notebook Marimo `anomaly_detector.py` vers topic dedie (`sensor.anomalies`)
- Ingestion streaming complementaire: notebook Marimo `field_file_producer.py` vers Kafka (`field.raw`)
- Detection d'anomalies complementaire: notebook Marimo `field_anomaly_detector.py` vers topic dedie (`field.anomalies`)
- Exposition des donnees: Kafka Connect S3 Sink vers MinIO
- Observabilite Kafka: Kafka UI (topics, messages, consumer groups, lags)

## Architecture

`capteur API -> notebook/api_producer.py -> Kafka topic sensor.raw -> notebook/anomaly_detector.py -> Kafka topic sensor.anomalies -> Kafka Connect S3 Sink -> MinIO`

`field_app (fichier NDJSON) -> notebook/field_file_producer.py -> Kafka topic field.raw -> notebook/field_anomaly_detector.py -> Kafka topic field.anomalies -> Kafka Connect S3 Sink -> MinIO`

## Prerequis

- Docker Desktop
- Docker Compose v2

## Demarrage

Depuis `kafka/`:

```bash
docker compose up -d --build
```

Verifier les services:

```bash
docker compose ps
```

## Interfaces

- API capteur: `http://localhost:5000/health`
- Marimo: `http://localhost:8080`
- Kafka Connect: `http://localhost:8083/connectors`
- Kafka UI: `http://localhost:8085`
- MinIO Console: `http://localhost:9001` (user: `minioadmin`, password: `minioadmin123`)

## Execution via Marimo (cellule par cellule)

Dans Marimo, ouvrir les scripts de `notebook/`:

- `produce.py`: envoi unitaire vers `sensor.raw`
- `api_producer.py`: envoi en lot/continu depuis l'API vers `sensor.raw`
- `anomaly_detector.py`: consomme `sensor.raw` et publie les anomalies vers `sensor.anomalies`
- `field_file_producer.py`: lit `data/field_events.ndjson` et publie vers `field.raw`
- `field_anomaly_detector.py`: consomme `field.raw` et publie les anomalies vers `field.anomalies`
- `consumer.py`: consomme et affiche les messages de `sensor.anomalies` (ou `sensor.raw`)
- `field_consumer.py`: consomme et affiche les messages de `field.anomalies` (ou `field.raw`)

Tous les notebooks incluent des cellules Markdown explicatives:
- role technique de chaque cellule
- justification metier associee

## Topics utilises

- `sensor.raw`: donnees brutes issues de l'API
- `sensor.anomalies`: evenements anormaux detectes en temps reel
- `field.raw`: donnees brutes agronomiques issues du fichier NDJSON
- `field.anomalies`: anomalies detectees sur les evenements agronomiques

## Connecteurs enregistres automatiquement

- `minio-sink-sensor-raw`
- `minio-sink-sensor-anomalies`
- `minio-sink-field-raw`
- `minio-sink-field-anomalies`

Les fichiers de config sont dans `connect/connectors/`.

## Mapping avec la consigne

- Source temps reel 1: API capteur simulee
- Source temps reel 2: application fichier append-only toutes les 30 secondes
- Source batch: deja existante dans le projet principal (PostgreSQL + traitements quotidiens)
- 3e source: objets S3 MinIO (historisation/exposition)
- Detection d'anomalies en temps reel: notebooks `anomaly_detector.py` et `field_anomaly_detector.py`
- Topics dedies anomalies: `sensor.anomalies`, `field.anomalies`
- Exposition des donnees: MinIO via Kafka Connect

## Axes d'amelioration

- Remplacer le producer notebook par un HTTP Source Connector pour faire API -> Kafka 100% Connect
- Ajouter un Schema Registry + Avro/Protobuf
- Ajouter Airflow pour orchestrer le batch quotidien et la supervision
- Ajouter un service de monitoring (Prometheus + Grafana)
- Ajouter des tests de non-regression sur les regles d'anomalies

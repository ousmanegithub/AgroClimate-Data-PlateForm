import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os
    import time
    from datetime import datetime, timezone

    import marimo as mo
    import requests
    from kafka import KafkaProducer

    return KafkaProducer, datetime, json, mo, os, requests, time, timezone


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 1 - Imports du producer API
    Cette cellule charge les bibliotheques reseau, Kafka et horodatage.

    **Justification metier**: on veut tracer chaque evenement avec un timestamp d'ingestion pour l'audit temps reel.
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    SENSOR_API_URL = os.getenv("SENSOR_API_URL", "http://localhost:5000/sensor")
    RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "sensor.raw")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- SENSOR_API_URL: {SENSOR_API_URL}")
    print(f"- RAW_TOPIC: {RAW_TOPIC}")
    return KAFKA_BOOTSTRAP, RAW_TOPIC, SENSOR_API_URL


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule lit les variables d'environnement pour connecter API et Kafka.

    **Justification metier**: separer configuration et logique facilite le deploiement dev/prod.
    """)
    return


@app.cell
def _(KAFKA_BOOTSTRAP, KafkaProducer, json):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"KafkaProducer initialise sur {KAFKA_BOOTSTRAP}")
    return (producer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 3 - Initialisation producer
    Cette cellule prepare le producteur Kafka JSON.

    **Justification metier**: standardiser le format JSON accelere l'integration avec les consumers et Kafka Connect.
    """)
    return


@app.cell
def _(SENSOR_API_URL, requests):
    def fetch_sensor_data() -> dict:
        response = requests.get(SENSOR_API_URL, timeout=5)
        response.raise_for_status()
        return response.json()

    print("Fonction fetch_sensor_data() prete")
    return (fetch_sensor_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Lecture de la source API
    Cette cellule encapsule l'appel HTTP a la source capteur.

    **Justification metier**: centraliser la lecture facilite la gestion des erreurs et l'evolution des endpoints.
    """)
    return


@app.cell
def _(RAW_TOPIC, datetime, fetch_sensor_data, producer, time, timezone):
    def produce_sensor_events(num_messages: int = 20, interval_seconds: int = 1) -> list[dict]:
        print(f"Production de {num_messages} messages vers '{RAW_TOPIC}'...")
        produced_events = []

        for i in range(num_messages):
            payload = fetch_sensor_data()
            payload["ingested_at"] = datetime.now(timezone.utc).isoformat()

            producer.send(RAW_TOPIC, payload)
            produced_events.append(payload)

            if (i + 1) % 10 == 0 or (i + 1) == num_messages:
                print(f"  - {i + 1} messages produits")

            if i < num_messages - 1 and interval_seconds > 0:
                time.sleep(interval_seconds)

        producer.flush()
        print(f"Termine: {len(produced_events)} messages envoyes vers '{RAW_TOPIC}'")
        return produced_events

    return (produce_sensor_events,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Publication en lot
    Cette cellule pousse un lot de messages dans `sensor.raw`.

    **Justification metier**: simuler un flux continu permet de tester la detection d'anomalies en conditions proches du reel.
    """)
    return


@app.cell
def _(produce_sensor_events):
    events = produce_sensor_events(num_messages=30, interval_seconds=1)
    print("Exemple payload:", events[0] if events else "aucun")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 6 - Execution
    Cette cellule lance effectivement la production.

    **Justification metier**: fournir un point d'execution unique simplifie les demonstrations et la reproductibilite.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

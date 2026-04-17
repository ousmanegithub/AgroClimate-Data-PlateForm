import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os

    import marimo as mo
    import requests
    from kafka import KafkaProducer

    return KafkaProducer, json, mo, os, requests


@app.cell
def _(mo):
    mo.md(
        """
        ## Cellule 1 - Contexte et imports
        Cette cellule charge les dependances pour publier un evenement capteur unique.

        **Justification metier**: utile pour un test rapide de bout en bout (API -> Kafka) avant les envois en masse.
        """
    )
    return


@app.cell
def _(mo, os):
    api_url = mo.ui.text(value=os.getenv("SENSOR_API_URL", "http://capteur:5000/sensor"), label="Sensor API URL")
    bootstrap = mo.ui.text(value=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), label="Kafka bootstrap")
    topic = mo.ui.text(value=os.getenv("KAFKA_TOPIC_RAW", "sensor.raw"), label="Topic")

    mo.vstack([api_url, bootstrap, topic])
    return api_url, bootstrap, topic


@app.cell
def _(mo):
    mo.md(
        """
        ## Cellule 2 - Parametrage manuel
        Cette cellule expose les parametres de connexion API/Kafka.

        **Justification metier**: permet de basculer rapidement entre environnements sans modifier le code.
        """
    )
    return


@app.cell
def _(KafkaProducer, api_url, bootstrap, json, mo, requests, topic):
    button = mo.ui.run_button(label="Fetch API + Publish to Kafka")

    payload = None
    if button.value:
        payload = requests.get(api_url.value, timeout=5).json()
        producer = KafkaProducer(
            bootstrap_servers=bootstrap.value,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        producer.send(topic.value, payload)
        producer.flush()

    mo.vstack(
        [
            button,
            mo.md(f"Published 1 event to `{topic.value}`" if payload else "Click the button to publish one message."),
            payload if payload else "",
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Cellule 3 - Publication unitaire
        Cette cellule lit un evenement de l'API puis le publie dans Kafka.

        **Justification metier**: validation minimale de la chaine de streaming avant execution des jobs complets.
        """
    )
    return


if __name__ == "__main__":
    app.run()

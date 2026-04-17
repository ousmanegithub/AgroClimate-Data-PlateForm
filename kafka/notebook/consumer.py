import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os

    import marimo as mo
    from kafka import KafkaConsumer, TopicPartition

    return KafkaConsumer, TopicPartition, json, mo, os


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 1 - Imports consumer capteur
    Cette cellule charge les composants pour lire des topics Kafka.

    **Justification metier**: rendre visibles les messages facilite le controle qualite du flux streaming.
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "sensor.raw")
    ANOMALY_TOPIC = os.getenv("KAFKA_TOPIC_ANOMALY", "sensor.anomalies")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "agro-consumer-v1")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- RAW_TOPIC: {RAW_TOPIC}")
    print(f"- ANOMALY_TOPIC: {ANOMALY_TOPIC}")
    print(f"- CONSUMER_GROUP: {CONSUMER_GROUP}")
    return ANOMALY_TOPIC, CONSUMER_GROUP, KAFKA_BOOTSTRAP


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule fixe la cible de lecture (brut ou anomalies).

    **Justification metier**: pouvoir lire les deux topics permet controle operationnel complet.
    """)
    return


@app.cell
def _(CONSUMER_GROUP, KAFKA_BOOTSTRAP, KafkaConsumer, json):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    print(f"KafkaConsumer initialise sur {KAFKA_BOOTSTRAP}")
    return (consumer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 3 - Initialisation consumer
    Cette cellule cree un consumer generique reutilisable.

    **Justification metier**: eviter la duplication accelere les tests sur plusieurs topics.
    """)
    return


@app.cell
def _(TopicPartition, consumer):
    def consume_topic_messages(topic_name: str, max_records: int = 100, from_beginning: bool = True) -> list[dict]:
        print(f"Lecture du topic '{topic_name}'...")
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            print(f"Aucune partition trouvee pour '{topic_name}'")
            return []

        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        consumer.assign(topic_partitions)

        if from_beginning:
            consumer.seek_to_beginning(*topic_partitions)
            print("Offsets repositionnes au debut du topic")

        messages = []
        for message in consumer:
            payload = message.value
            messages.append(
                {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "payload": payload,
                }
            )
            if len(messages) >= max_records:
                break

        print(f"Termine: {len(messages)} messages lus sur '{topic_name}'")
        return messages

    return (consume_topic_messages,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Fonction de consommation
    Cette cellule lit un topic avec controle des offsets et du volume.

    **Justification metier**: garantir un echantillon lisible pour diagnostic et monitoring.
    """)
    return


@app.cell
def _(ANOMALY_TOPIC, consume_topic_messages):
    anomaly_messages = consume_topic_messages(
        topic_name=ANOMALY_TOPIC,
        max_records=50,
        from_beginning=True,
    )
    print("Exemple message:", anomaly_messages[0] if anomaly_messages else "aucun message")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Execution
    Cette cellule lit les anomalies capteur pour verification.

    **Justification metier**: un point de controle rapide pour valider la qualite des alertes.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

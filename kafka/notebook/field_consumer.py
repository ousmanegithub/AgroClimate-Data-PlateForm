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
    ## Cellule 1 - Imports consumer source 2
    Cette cellule charge les composants pour lire les topics terrain.

    **Justification metier**: verifier la qualite des donnees complementaires (field events) avant analyses unifiees.
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    FIELD_RAW_TOPIC = os.getenv("KAFKA_TOPIC_FIELD_RAW", "field.raw")
    FIELD_ANOMALY_TOPIC = os.getenv("KAFKA_TOPIC_FIELD_ANOMALY", "field.anomalies")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "field-consumer-v1")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- FIELD_RAW_TOPIC: {FIELD_RAW_TOPIC}")
    print(f"- FIELD_ANOMALY_TOPIC: {FIELD_ANOMALY_TOPIC}")
    print(f"- CONSUMER_GROUP: {CONSUMER_GROUP}")
    return CONSUMER_GROUP, FIELD_ANOMALY_TOPIC, KAFKA_BOOTSTRAP


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule definit les topics de la 2e source.

    **Justification metier**: distinguer le flux brut terrain du flux d'alertes terrain.
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
    Cette cellule prepare la lecture des topics field.

    **Justification metier**: permettre un controle dedie a la source complementaire.
    """)
    return


@app.cell
def _(TopicPartition, consumer):
    def consume_field_topic(topic_name: str, max_records: int = 100, from_beginning: bool = True) -> list[dict]:
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
            messages.append(
                {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "payload": message.value,
                }
            )
            if len(messages) >= max_records:
                break

        print(f"Termine: {len(messages)} messages lus sur '{topic_name}'")
        return messages

    return (consume_field_topic,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Fonction de lecture field
    Cette cellule standardise la lecture des messages de la 2e source.

    **Justification metier**: fiabiliser le diagnostic du flux terrain avant consolidation multi-sources.
    """)
    return


@app.cell
def _(FIELD_ANOMALY_TOPIC, consume_field_topic):
    messages = consume_field_topic(topic_name=FIELD_ANOMALY_TOPIC, max_records=50, from_beginning=True)
    print("Exemple message field:", messages[0] if messages else "aucun message")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Execution
    Cette cellule lit les anomalies terrain pour verification operationnelle.

    **Justification metier**: controler rapidement les alertes de la 2e source avant analyse metier unifiee.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os
    from datetime import datetime, timezone

    import marimo as mo
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition

    return (
        KafkaConsumer,
        KafkaProducer,
        TopicPartition,
        datetime,
        json,
        mo,
        os,
        timezone,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 1 - Imports du detecteur d anomalies capteur
    Cette cellule charge les composants Kafka et les utilitaires de date.

    **Justification metier**: chaque anomalie doit etre horodatee pour pilotage et tracabilite.
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "sensor.raw")
    ANOMALY_TOPIC = os.getenv("KAFKA_TOPIC_ANOMALY", "sensor.anomalies")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "anomaly-detector-v1")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- RAW_TOPIC: {RAW_TOPIC}")
    print(f"- ANOMALY_TOPIC: {ANOMALY_TOPIC}")
    print(f"- CONSUMER_GROUP: {CONSUMER_GROUP}")
    return ANOMALY_TOPIC, CONSUMER_GROUP, KAFKA_BOOTSTRAP, RAW_TOPIC


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule definit topics et consumer group.

    **Justification metier**: separer topic brut et topic anomalie structure la surveillance operationnelle.
    """)
    return


@app.cell
def _(CONSUMER_GROUP, KAFKA_BOOTSTRAP, KafkaConsumer, RAW_TOPIC, json):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    print(f"KafkaConsumer initialise sur '{RAW_TOPIC}' avec group '{CONSUMER_GROUP}'")
    return (consumer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 3 - Consumer source capteur
    Cette cellule cree le consumer sur le topic brut.

    **Justification metier**: lire le flux brut permet d identifier rapidement les signaux a risque.
    """)
    return


@app.cell
def _(ANOMALY_TOPIC, KAFKA_BOOTSTRAP, KafkaProducer, json):
    anomaly_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    print(f"KafkaProducer initialise sur '{ANOMALY_TOPIC}'")
    return (anomaly_producer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Producer anomalies
    Cette cellule prepare l emission vers le topic dedie aux anomalies.

    **Justification metier**: isoler les anomalies accelere les actions terrain et les alertes.
    """)
    return


@app.cell
def _():
    def detect_anomalies(payload: dict) -> list[str]:
        reasons: list[str] = []

        if payload.get("temperature_air", 0) > 38:
            reasons.append("temperature_air_gt_38")
        if payload.get("humidite_sol", 100) < 20:
            reasons.append("humidite_sol_lt_20")
        if payload.get("indice_stress_hydrique", 0) > 1.8:
            reasons.append("indice_stress_hydrique_gt_1_8")

        ph = payload.get("ph_sol")
        if ph is not None and (ph < 5.5 or ph > 7.8):
            reasons.append("ph_sol_out_of_range")

        return reasons

    print("Regles de detection chargees")
    return (detect_anomalies,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Regles metier d anomalie
    Cette cellule encode les seuils de risque agronomique.

    **Justification metier**: traduire les seuils metier en regles automatiques permet un monitoring temps reel.
    """)
    return


@app.cell
def _(
    ANOMALY_TOPIC,
    RAW_TOPIC,
    TopicPartition,
    anomaly_producer,
    consumer,
    datetime,
    detect_anomalies,
    timezone,
):
    def run_anomaly_detector(max_records: int = 100) -> list[dict]:
        consumed = 0
        anomalies = []

        print(f"Lecture de '{RAW_TOPIC}' et publication vers '{ANOMALY_TOPIC}'")
        partitions = consumer.partitions_for_topic(RAW_TOPIC)
        if not partitions:
            print(f"Aucune partition disponible pour '{RAW_TOPIC}'")
            return anomalies

        topic_partitions = [TopicPartition(RAW_TOPIC, p) for p in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_beginning(*topic_partitions)
        print("Offsets repositionnes au debut du topic")

        for message in consumer:
            payload = message.value
            consumed += 1

            reasons = detect_anomalies(payload)
            if reasons:
                anomaly_event = {
                    "source_topic": RAW_TOPIC,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "reasons": reasons,
                    "payload": payload,
                }
                anomaly_producer.send(ANOMALY_TOPIC, anomaly_event)
                anomalies.append(anomaly_event)

            if consumed >= max_records:
                break

        anomaly_producer.flush()
        print(f"Termine: {consumed} messages lus, {len(anomalies)} anomalies publiees")
        return anomalies

    return (run_anomaly_detector,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 6 - Process detection et routage
    Cette cellule consomme le brut, detecte, puis republie les anomalies.

    **Justification metier**: ce routage alimente un canal prioritaire pour supervision et reaction rapide.
    """)
    return


@app.cell
def _(run_anomaly_detector):
    anomalies = run_anomaly_detector(max_records=150)
    print("Exemple anomalie:", anomalies[0] if anomalies else "aucune anomalie detectee")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 7 - Execution TP
    Cette cellule lance la detection sur un volume fixe.

    **Justification metier**: une execution reproductible facilite la validation fonctionnelle en demo.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

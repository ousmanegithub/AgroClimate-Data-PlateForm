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
    ## Cellule 1 - Imports detecteur anomalies source 2
    Cette cellule charge les composants Kafka pour la chaine field events.

    **Justification metier**: surveiller les risques agronomiques operationnels (maladies, sur-irrigation, etc.).
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    FIELD_RAW_TOPIC = os.getenv("KAFKA_TOPIC_FIELD_RAW", "field.raw")
    FIELD_ANOMALY_TOPIC = os.getenv("KAFKA_TOPIC_FIELD_ANOMALY", "field.anomalies")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "field-anomaly-detector-v1")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- FIELD_RAW_TOPIC: {FIELD_RAW_TOPIC}")
    print(f"- FIELD_ANOMALY_TOPIC: {FIELD_ANOMALY_TOPIC}")
    print(f"- CONSUMER_GROUP: {CONSUMER_GROUP}")
    return (
        CONSUMER_GROUP,
        FIELD_ANOMALY_TOPIC,
        FIELD_RAW_TOPIC,
        KAFKA_BOOTSTRAP,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule configure le topic brut et le topic anomalies de la 2e source.

    **Justification metier**: separer evenements et alertes pour prioriser les actions terrain.
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
    ## Cellule 3 - Consumer field.raw
    Cette cellule initialise la consommation des evenements terrain.

    **Justification metier**: analyser rapidement la situation des parcelles et interventions.
    """)
    return


@app.cell
def _(FIELD_ANOMALY_TOPIC, KAFKA_BOOTSTRAP, KafkaProducer, json):
    anomaly_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"KafkaProducer initialise sur '{FIELD_ANOMALY_TOPIC}'")
    return (anomaly_producer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Producer field.anomalies
    Cette cellule prepare la publication des alertes terrain.

    **Justification metier**: concentrer les cas critiques pour faciliter la prise de decision.
    """)
    return


@app.cell
def _():
    def detect_field_anomalies(payload: dict) -> list[str]:
        reasons: list[str] = []

        if payload.get("presence_maladie") and payload.get("niveau_infestation") == "fort":
            reasons.append("maladie_forte")

        if payload.get("presence_maladie") and not payload.get("intervention_humaine"):
            reasons.append("maladie_sans_intervention")

        if payload.get("presence_maladie") and payload.get("intervention_humaine") == "aucune":
            reasons.append("maladie_non_traitee")

        if payload.get("dose_irrigation_mm", 0) > 25:
            reasons.append("sur_irrigation")

        if payload.get("engrais_applique_kg", 0) > 22:
            reasons.append("sur_dose_engrais")

        if payload.get("stade_croissance") in {"semis", "croissance"} and not payload.get("irrigation_active"):
            reasons.append("stress_hydrique_potentiel")

        return reasons

    print("Regles de detection chargees")
    return (detect_field_anomalies,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Regles anomalies terrain
    Cette cellule formalise les risques metier lies aux pratiques culturales.

    **Justification metier**: declencher des recommandations precoces pour limiter pertes de rendement.
    """)
    return


@app.cell
def _(
    FIELD_ANOMALY_TOPIC,
    FIELD_RAW_TOPIC,
    TopicPartition,
    anomaly_producer,
    consumer,
    datetime,
    detect_field_anomalies,
    timezone,
):
    def run_field_anomaly_detector(max_records: int = 150) -> list[dict]:
        consumed = 0
        anomalies = []

        print(f"Lecture de '{FIELD_RAW_TOPIC}' et publication vers '{FIELD_ANOMALY_TOPIC}'")
        partitions = consumer.partitions_for_topic(FIELD_RAW_TOPIC)
        if not partitions:
            print(f"Aucune partition disponible pour '{FIELD_RAW_TOPIC}'")
            return anomalies

        topic_partitions = [TopicPartition(FIELD_RAW_TOPIC, p) for p in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_beginning(*topic_partitions)
        print("Offsets repositionnes au debut du topic")

        for message in consumer:
            payload = message.value
            consumed += 1

            reasons = detect_field_anomalies(payload)
            if reasons:
                anomaly_event = {
                    "source_topic": FIELD_RAW_TOPIC,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "reasons": reasons,
                    "payload": payload,
                }
                anomaly_producer.send(FIELD_ANOMALY_TOPIC, anomaly_event)
                anomalies.append(anomaly_event)

            if consumed >= max_records:
                break

        anomaly_producer.flush()
        print(f"Termine: {consumed} messages lus, {len(anomalies)} anomalies publiees")
        return anomalies

    return (run_field_anomaly_detector,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 6 - Process detection source 2
    Cette cellule consomme `field.raw` et route les alertes vers `field.anomalies`.

    **Justification metier**: automatiser la priorisation des parcelles a risque.
    """)
    return


@app.cell
def _(run_field_anomaly_detector):
    anomalies = run_field_anomaly_detector(max_records=120)
    print("Exemple anomalie:", anomalies[0] if anomalies else "aucune anomalie detectee")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 7 - Execution
    Cette cellule lance l'analyse anomalies de la 2e source.

    **Justification metier**: verifier rapidement la pertinence des regles terrain en demonstration.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

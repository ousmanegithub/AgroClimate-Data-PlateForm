import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import os
    from pathlib import Path

    import marimo as mo
    from kafka import KafkaProducer

    return KafkaProducer, Path, json, mo, os


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 1 - Imports source fichier
    Cette cellule charge les outils fichier + Kafka producer.

    **Justification metier**: la 2e source provient d'une application qui ecrit en continu dans un fichier NDJSON.
    """)
    return


@app.cell
def _(os):
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    FIELD_DATA_FILE = os.getenv("FIELD_DATA_FILE", "/app/data/field_events.ndjson")
    FIELD_RAW_TOPIC = os.getenv("KAFKA_TOPIC_FIELD_RAW", "field.raw")

    print("Configuration chargee:")
    print(f"- KAFKA_BOOTSTRAP: {KAFKA_BOOTSTRAP}")
    print(f"- FIELD_DATA_FILE: {FIELD_DATA_FILE}")
    print(f"- FIELD_RAW_TOPIC: {FIELD_RAW_TOPIC}")
    return FIELD_DATA_FILE, FIELD_RAW_TOPIC, KAFKA_BOOTSTRAP


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 2 - Configuration
    Cette cellule pointe vers le fichier evenementiel et le topic cible.

    **Justification metier**: connecter la source operationnelle terrain au pipeline de streaming.
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
    Cette cellule prepare le producer pour `field.raw`.

    **Justification metier**: convertir une source fichier en flux Kafka exploitable en temps reel.
    """)
    return


@app.cell
def _(FIELD_DATA_FILE, Path, json):
    def read_field_events(max_records: int = 50, from_beginning: bool = False) -> list[dict]:
        file_path = Path(FIELD_DATA_FILE)
        if not file_path.exists():
            print(f"Fichier introuvable: {file_path}")
            return []

        with file_path.open("r", encoding="utf-8") as f:
            raw = f.read()

        if "\n" not in raw and "\\n" in raw:
            raw = raw.replace("\\n", "\n")

        lines = [line.strip() for line in raw.splitlines() if line.strip()]

        if not from_beginning:
            lines = lines[-max_records:]
        else:
            lines = lines[:max_records]

        events = []
        for line in lines:
            try:
                events.append(json.loads(line))
            except Exception as exc:  # noqa: BLE001
                print(f"Ligne ignoree (JSON invalide): {exc}")

        print(f"{len(events)} evenements charges depuis le fichier")
        return events

    return (read_field_events,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 4 - Lecture NDJSON
    Cette cellule lit et valide les evenements du fichier (avec compatibilite ancienne ecriture).

    **Justification metier**: eviter la perte de donnees historiques meme si le format fichier a evolue.
    """)
    return


@app.cell
def _(FIELD_RAW_TOPIC, producer, read_field_events):
    def produce_field_events(max_records: int = 50, from_beginning: bool = False) -> list[dict]:
        events = read_field_events(max_records=max_records, from_beginning=from_beginning)
        if not events:
            print("Aucun evenement a produire")
            return []

        print(f"Production de {len(events)} messages vers '{FIELD_RAW_TOPIC}'...")
        for i, event in enumerate(events, start=1):
            producer.send(FIELD_RAW_TOPIC, event)
            if i % 10 == 0 or i == len(events):
                print(f"  - {i} messages produits")

        producer.flush()
        print(f"Termine: {len(events)} messages envoyes vers '{FIELD_RAW_TOPIC}'")
        return events

    return (produce_field_events,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 5 - Publication source 2
    Cette cellule publie les evenements agronomiques vers Kafka.

    **Justification metier**: unifier les flux capteur et terrain dans la meme architecture de traitement.
    """)
    return


@app.cell
def _(produce_field_events):
    events = produce_field_events(max_records=40, from_beginning=False)
    print("Exemple payload:", events[0] if events else "aucun")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Cellule 6 - Execution
    Cette cellule declenche la publication de la 2e source.

    **Justification metier**: preuve de fonctionnement du flux complementaire dans la demo.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

REGIONS = [
    "Dakar",
    "Diourbel",
    "Fatick",
    "Kaffrine",
    "Kaolack",
    "Kedougou",
    "Kolda",
    "Louga",
    "Matam",
    "Saint-Louis",
    "Sedhiou",
    "Tambacounda",
    "Thies",
    "Ziguinchor",
]

CULTURES = ["riz", "mais", "arachide", "mil", "sorgho", "tomate", "oignon"]
STADES = ["semis", "croissance", "floraison", "recolte"]
MALADIES = ["rouille", "mildiou", "fusariose", "alternariose"]
INFESTATIONS = ["faible", "moyen", "fort"]
INTERVENTIONS = ["aucune", "traitement_fongicide", "traitement_biologique", "desherbage"]


def generate_field_event() -> dict:
    presence_maladie = random.random() < 0.22
    irrigation_active = random.random() < 0.65

    if presence_maladie:
        type_maladie = random.choice(MALADIES)
        niveau_infestation = random.choices(INFESTATIONS, weights=[0.35, 0.4, 0.25], k=1)[0]
    else:
        type_maladie = None
        niveau_infestation = "faible"

    dose_irrigation_mm = round(random.uniform(5, 30), 2) if irrigation_active else 0.0
    engrais_applique_kg = round(random.uniform(0, 25), 2)

    if presence_maladie and niveau_infestation == "fort":
        intervention_humaine = random.choice(["traitement_fongicide", "traitement_biologique"])
    else:
        intervention_humaine = random.choice(INTERVENTIONS)

    return {
        "event_id": str(uuid.uuid4()),
        "region": random.choice(REGIONS),
        "parcelle_id": f"PARC-{random.randint(1000, 9999)}",
        "culture": random.choice(CULTURES),
        "stade_croissance": random.choice(STADES),
        "irrigation_active": irrigation_active,
        "dose_irrigation_mm": dose_irrigation_mm,
        "engrais_applique_kg": engrais_applique_kg,
        "presence_maladie": presence_maladie,
        "type_maladie": type_maladie,
        "niveau_infestation": niveau_infestation,
        "intervention_humaine": intervention_humaine,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    output_file = os.getenv("FIELD_OUTPUT_FILE", "/data/field_events.ndjson")
    interval_seconds = float(os.getenv("FIELD_INTERVAL_SECONDS", "30"))

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[field-app] writing to {output_path}")
    print(f"[field-app] interval: {interval_seconds}s")

    while True:
        event = generate_field_event()
        with output_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=True) + "\n")
        print(f"[field-app] appended event_id={event['event_id']}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()

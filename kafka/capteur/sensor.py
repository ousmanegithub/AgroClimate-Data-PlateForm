import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone


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


def _compute_indice_stress_hydrique(evapotranspiration: float, pluviometrie: float) -> float:
    return round(max(0, (evapotranspiration - pluviometrie) / 10), 2)


class SensorStream:
    def __init__(self, interval_seconds: float = 1.0, max_history: int = 10000, anomaly_rate: float = 0.15):
        self.interval_seconds = interval_seconds
        self.max_history = max_history
        self.anomaly_rate = anomaly_rate

        self._history = deque(maxlen=max_history)
        self._lock = threading.Lock()
        self._thread = None
        self._stop_event = threading.Event()
        self._sequence = 0

    def _generate_baseline(self) -> dict:
        region = random.choice(REGIONS)

        temperature_air = round(random.uniform(20, 40), 2)
        humidite_air = round(random.uniform(30, 90), 2)
        vitesse_vent = round(random.uniform(1, 6), 2)
        rayonnement_solaire = round(random.uniform(15, 25), 2)

        pluviometrie = round(random.uniform(0, 20), 2)
        humidite_sol = round(min(100, pluviometrie * random.uniform(2, 4) + random.uniform(10, 40)), 2)
        evapotranspiration = round((temperature_air * 0.1) + (rayonnement_solaire * 0.05), 2)

        ph_sol = round(random.uniform(5.0, 8.0), 2)
        azote = round(random.uniform(10, 50), 2)
        phosphore = round(random.uniform(5, 30), 2)
        potassium = round(random.uniform(50, 200), 2)
        carbone_organique = round(random.uniform(0.5, 3.5), 2)
        temperature_sol = round(temperature_air - random.uniform(1, 5), 2)
        indice_stress_hydrique = _compute_indice_stress_hydrique(evapotranspiration, pluviometrie)

        return {
            "sequence": self._sequence,
            "region": region,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature_air": temperature_air,
            "humidite_air": humidite_air,
            "vitesse_vent": vitesse_vent,
            "rayonnement_solaire": rayonnement_solaire,
            "pluviometrie": pluviometrie,
            "humidite_sol": humidite_sol,
            "evapotranspiration": evapotranspiration,
            "ph_sol": ph_sol,
            "azote": azote,
            "phosphore": phosphore,
            "potassium": potassium,
            "carbone_organique": carbone_organique,
            "temperature_sol": temperature_sol,
            "indice_stress_hydrique": indice_stress_hydrique,
            "is_anomaly": False,
            "anomaly_types": [],
        }

    def _inject_anomaly(self, payload: dict) -> dict:
        anomaly_types = []

        choice = random.choice(["temperature", "humidite_sol", "ph_sol", "stress_combo"])

        if choice == "temperature":
            payload["temperature_air"] = round(random.uniform(42, 52), 2)
            payload["temperature_sol"] = round(payload["temperature_air"] - random.uniform(0, 1), 2)
            anomaly_types.append("temperature_air_extreme")
        elif choice == "humidite_sol":
            payload["humidite_sol"] = round(random.uniform(5, 18), 2)
            anomaly_types.append("humidite_sol_tres_basse")
        elif choice == "ph_sol":
            payload["ph_sol"] = round(random.choice([random.uniform(3.8, 5.2), random.uniform(8.2, 9.2)]), 2)
            anomaly_types.append("ph_sol_hors_plage")
        else:
            payload["temperature_air"] = round(random.uniform(40, 48), 2)
            payload["pluviometrie"] = round(random.uniform(0, 2), 2)
            payload["humidite_sol"] = round(random.uniform(8, 18), 2)
            anomaly_types.append("stress_hydrique_extreme")

        payload["evapotranspiration"] = round(
            (payload["temperature_air"] * 0.1) + (payload["rayonnement_solaire"] * 0.05),
            2,
        )
        payload["indice_stress_hydrique"] = _compute_indice_stress_hydrique(
            payload["evapotranspiration"],
            payload["pluviometrie"],
        )

        payload["is_anomaly"] = True
        payload["anomaly_types"] = anomaly_types
        return payload

    def _generate(self) -> dict:
        self._sequence += 1
        payload = self._generate_baseline()

        if random.random() < self.anomaly_rate:
            payload = self._inject_anomaly(payload)

        return payload

    def _run(self) -> None:
        while not self._stop_event.is_set():
            payload = self._generate()
            with self._lock:
                self._history.append(payload)
            time.sleep(self.interval_seconds)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def latest(self) -> dict | None:
        with self._lock:
            return self._history[-1] if self._history else None

    def history(self, n: int = 10) -> list[dict]:
        with self._lock:
            if n <= 0:
                return []
            return list(self._history)[-n:]

    def history_by_region(self, region: str, n: int = 10) -> list[dict]:
        with self._lock:
            filtered = [item for item in self._history if item["region"].lower() == region.lower()]
        return filtered[-n:] if n > 0 else []

    def stats(self) -> dict:
        with self._lock:
            total = len(self._history)
            anomalies = sum(1 for item in self._history if item.get("is_anomaly"))
            latest_sequence = self._history[-1]["sequence"] if self._history else 0

        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "interval_seconds": self.interval_seconds,
            "max_history": self.max_history,
            "total_generated_in_buffer": total,
            "anomaly_count_in_buffer": anomalies,
            "latest_sequence": latest_sequence,
        }


SENSOR_INTERVAL_SECONDS = float(os.getenv("SENSOR_INTERVAL_SECONDS", "1"))
SENSOR_MAX_HISTORY = int(os.getenv("SENSOR_MAX_HISTORY", "10000"))
SENSOR_ANOMALY_RATE = float(os.getenv("SENSOR_ANOMALY_RATE", "0.15"))

sensor_stream = SensorStream(
    interval_seconds=SENSOR_INTERVAL_SECONDS,
    max_history=SENSOR_MAX_HISTORY,
    anomaly_rate=SENSOR_ANOMALY_RATE,
)


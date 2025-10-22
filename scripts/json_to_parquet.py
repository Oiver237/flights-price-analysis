import json
import pandas as pd

# --- Charger le JSON ---
json_file = "data/sample_result.json"
with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# --- Extraire les vols ---
vols_list = []

# On parcourt à la fois best_flights et other_flights
for flight_entry in data.get("best_flights", []) + data.get("other_flights", []):
    flights = flight_entry.get("flights", [])
    if not flights:
        continue

    # Itinéraire
    departure = flights[0]["departure_airport"]["id"]
    arrival = flights[-1]["arrival_airport"]["id"]

    # Dates
    outbound_date = flights[0]["departure_airport"]["time"].split(" ")[0]
    return_date = flights[-1]["arrival_airport"]["time"].split(" ")[0]  # approximatif

    # Prix et durée
    price = flight_entry.get("price")
    total_duration = flight_entry.get("total_duration")

    # Compagnie aérienne (premier segment représentatif)
    airline = flights[0].get("airline") or "Inconnue"

    vols_list.append({
        "departure": departure,
        "arrival": arrival,
        "outbound_date": outbound_date,
        "return_date": return_date,
        "price": price,
        "duration_min": total_duration,
        "airline": airline
    })

# --- Créer DataFrame ---
df = pd.DataFrame(vols_list)

# --- Vérification rapide ---
print(df.head())

# --- Sauvegarder en Parquet ---
parquet_file = "data/vols.parquet"
df.to_parquet(parquet_file, index=False)
print(f"Fichier Parquet créé avec succès : {parquet_file}")

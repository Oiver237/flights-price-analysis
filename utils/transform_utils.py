# utils/transform_utils.py
import pandas as pd
import json

def json_to_parquet(json_file, parquet_file="vols.parquet"):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    flights = data.get("best_flights", [])
    df = pd.DataFrame([
        {
            "departure": f.get("departure") or f.get("from", "N/A"),
            "arrival": f.get("arrival") or f.get("to", "N/A"),
            "outbound_date": f.get("outbound_date") or f.get("out", "N/A"),
            "return_date": f.get("return_date") or f.get("ret", "N/A"),
            "price": f.get("price", "N/A"),
            "duration_min": f.get("total_duration", "N/A"),
            "airline": ", ".join([seg.get("airline", "N/A") for seg in f.get("segments", [])])
        }
        for f in flights
    ])
    df.to_parquet(parquet_file, engine='pyarrow', index=False)
    print(f"Fichier Parquet créé: {parquet_file}")
    return parquet_file

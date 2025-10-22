import os
import sys
import csv
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import itertools

# --- Ajouter le dossier parent au PYTHONPATH ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform_utils import json_to_parquet
from utils.serpapi_utils import try_search_with_fallback, extract_airlines

# --- Charger les variables d'environnement ---
load_dotenv()

AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

SERPAPI_KEYS = [os.getenv("SERPAPI_API_KEY"), os.getenv("SERPAPI_API_KEY_2")]
SERPAPI_KEYS = [k for k in SERPAPI_KEYS if k]

# --- Initialiser le client S3 ---
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def check_s3_access(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Accès S3 vérifié pour le bucket '{bucket_name}'")
        return True
    except ClientError as e:
        print(f"Erreur d'accès S3: {e}")
        return False

def upload_to_s3(filename, bucket, key):
    try:
        s3_client.upload_file(filename, bucket, key)
        print(f"Fichier {filename} envoyé sur S3: {bucket}/{key}")
    except ClientError as e:
        print(f"Erreur lors de l'upload S3: {e}")

def save_to_csv(flights, filename="vols.csv"):
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["Départ", "Arrivée", "Date aller", "Date retour", "Prix (€)", "Durée (min)", "Compagnie"])
        for f in flights:
            writer.writerow([f["from"], f["to"], f["out"], f["ret"], f["price"], f["total_duration"], f["airline"]])

# --- Vérifications initiales ---
if not check_s3_access(AWS_BUCKET):
    print("Impossible d'accéder au bucket S3. Vérifie tes credentials AWS.")
    exit(1)

if not SERPAPI_KEYS:
    print("Aucune clé SerpAPI valide disponible. Vérifie ton fichier .env.")
    exit(1)

# --- Configuration des vols ---
departures = ["CDG", "ORY"]
arrivals = ["BER", "TXL"]
start_date = datetime.today() + timedelta(days=30)
date_range = 10
return_offset = 5

# --- Pipeline principal ---
key_cycle = itertools.cycle(SERPAPI_KEYS)
found = False

for dep in departures:
    for arr in arrivals:
        for i in range(date_range):
            outbound = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            return_date = (start_date + timedelta(days=i + return_offset)).strftime("%Y-%m-%d")
            print(f"Recherche: {dep} -> {arr}, Aller: {outbound}, Retour: {return_date}")

            # Rotation entre les clés SerpAPI
            for _ in range(len(SERPAPI_KEYS)):
                api_key = next(key_cycle)
                res = try_search_with_fallback(dep, arr, outbound, return_date)
                if res:
                    break
            else:
                print("Aucune clé SerpAPI n'a fonctionné pour cette recherche.")
                continue

            if res.get("flights_results_state") != "Fully empty":
                json_file = "sample_result.json"
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(res, f, ensure_ascii=False, indent=2)
                print(f"Résultats sauvegardés dans {json_file}")

                # Upload JSON vers S3
                upload_to_s3(json_file, AWS_BUCKET, f"raw/{json_file}")

                # Extraire les meilleurs vols
                best = res.get("best_flights") or []
                simple_results = []
                for idx, f in enumerate(best[:3]):
                    price = f.get("price") or "N/A"
                    duration = f.get("total_duration") or "N/A"
                    airline = extract_airlines(f)
                    simple_results.append({
                        "from": dep,
                        "to": arr,
                        "out": outbound,
                        "ret": return_date,
                        "price": price,
                        "total_duration": duration,
                        "airline": airline
                    })
                    print(f"Vol {idx+1}: prix={price}, durée={duration}, compagnie={airline}")

                # Sauvegarde CSV
                save_to_csv(simple_results)
                print("Fichier CSV mis à jour.")

                # Conversion en Parquet
                parquet_file = json_to_parquet(json_file)
                print(f"Fichier Parquet créé: {parquet_file}")

                # Upload Parquet vers S3
                upload_to_s3(parquet_file, AWS_BUCKET, f"parquet/{parquet_file}")

                found = True
                break
            else:
                print("Aucun vol trouvé pour cette combinaison.")
        if found:
            break
    if found:
        break

if not found:
    print("Aucun vol trouvé sur toutes les combinaisons testées.")
else:
    print("Pipeline exécuté avec succès.")

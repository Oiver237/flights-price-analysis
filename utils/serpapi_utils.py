import os
import time
from serpapi import GoogleSearch
from dotenv import load_dotenv

load_dotenv()

API_KEY_1 = os.getenv("SERPAPI_API_KEY")
API_KEY_2 = os.getenv("SERPAPI_API_KEY_2")

def search_flights(api_key, dep, arr, outbound_date, return_date):
    """Recherche de vols via SerpAPI avec les bons paramètres."""
    params = {
        "engine": "google_flights",
        "departure_id": dep,
        "arrival_id": arr,
        "outbound_date": outbound_date,
        "return_date": return_date,
        "currency": "EUR",
        "hl": "fr",
        "api_key": api_key
    }

    search = GoogleSearch(params)
    return search.get_dict()


def try_search_with_fallback(dep, arr, outbound_date, return_date):
    """Essaye successivement les clés SerpAPI."""
    for api_key in [API_KEY_1, API_KEY_2]:
        if not api_key:
            continue
        try:
            print(f"Tentative avec la clé SerpAPI: {api_key[:6]}...")
            result = search_flights(api_key, dep, arr, outbound_date, return_date)
            if result and result.get("search_metadata"):
                return result
        except Exception as e:
            print(f"Erreur avec la clé {api_key[:6]}: {e}")
        time.sleep(1)
    print("Aucune clé API valide ou plus de quota disponible.")
    return None


def extract_airlines(flight):
    """Extraction des noms des compagnies aériennes."""
    try:
        airlines = []
        if "airline" in flight:
            airlines.append(flight["airline"])
        elif "flights" in flight:
            for leg in flight["flights"]:
                if "airline" in leg:
                    airlines.append(leg["airline"])
        return ", ".join(set(airlines)) if airlines else "Inconnue"
    except Exception:
        return "Inconnue"

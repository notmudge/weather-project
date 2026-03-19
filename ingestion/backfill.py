import os, json, time
from datetime import date, timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import requests

load_dotenv()

STORAGE_CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER        = "bronze"
BASE_URL         = "https://archive-api.open-meteo.com/v1/archive"

VARIABLES = [
    "temperature_2m", "apparent_temperature",
    "precipitation",  "rain", "snowfall",
    "windspeed_10m",  "windgusts_10m",
    "cloudcover",     "weathercode",
    "relativehumidity_2m",
]

def fetch_weather(name, lat, lon, target_date):
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": str(target_date),
        "end_date":   str(target_date),
        "hourly":     ",".join(VARIABLES),
        "timezone":   "Europe/London",
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    payload["_meta"] = {
        "location_name": name,
        "ingested_at":   date.today().isoformat(),
    }
    return payload

def upload_to_bronze(blob_client, name, target_date, payload):
    path = (
        f"weather/year={target_date.year}"
        f"/month={target_date.month:02d}"
        f"/day={target_date.day:02d}"
        f"/{name}.json"
    )
    blob = blob_client.get_blob_client(container=CONTAINER, blob=path)
    blob.upload_blob(json.dumps(payload), overwrite=True)

def main():
    START_DATE = date(2024, 1, 1)
    END_DATE   = date.today() - timedelta(days=1)

    blob_svc = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

    with open("ingestion/locations.json") as f:
        locations = json.load(f)

    current = START_DATE
    total   = (END_DATE - START_DATE).days + 1
    done    = 0

    while current <= END_DATE:
        for loc in locations:
            try:
                payload = fetch_weather(loc["name"], loc["lat"], loc["lon"], current)
                upload_to_bronze(blob_svc, loc["name"], current, payload)
                time.sleep(0.1)
            except Exception as e:
                print(f"  ✗ {loc['name']} {current}: {e}")
        done += 1
        print(f"  ✓ {current} ({done}/{total})")
        current += timedelta(days=1)

    print("Backfill complete!")

if __name__ == "__main__":
    main()
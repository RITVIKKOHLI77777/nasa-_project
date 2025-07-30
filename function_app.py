import logging
import requests
from dotenv import load_dotenv
import os
import pandas as pd
import datetime
from azure.functions import TimerRequest
from azure.functions.decorators import FunctionApp
from azure.functions.decorators.timer import TimerTrigger
from azure.storage.blob import BlobServiceClient, ContentSettings

# === CONFIG ===
api_key = "a2eybtmPBvVIxbVSgXUbUOP4o0tyZrZqE1N3edQH"
container_name = "neo-container"
load_dotenv()
blob_connection_string = os.getenv("AZURE_STORAGE_KEY")

function_app = FunctionApp()

@function_app.function_name(name="GetNeoData")
@function_app.schedule(schedule="0 0 0 * * 1", arg_name="mytimer", run_on_startup=True, use_monitor=True)
def get_neo_data(mytimer: TimerRequest):
    utc_timestamp = datetime.datetime.utcnow().isoformat()
    logging.info(f"Function ran at {utc_timestamp}")

    try:
        today = datetime.datetime.utcnow()
        start_date = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"
        response = requests.get(url)
        data = response.json()

        asteroids = []
        for date, neos in data.get("near_earth_objects", {}).items():
            for obj in neos:
                approach = obj.get("close_approach_data", [{}])[0]
                asteroids.append({
                    "id": obj.get("id", ""),
                    "name": obj.get("name", ""),
                    "neo_reference_id": obj.get("neo_reference_id", ""),
                    "absolute_magnitude_h": obj.get("absolute_magnitude_h", ""),
                    "estimated_diameter_min_m": obj.get("estimated_diameter", {}).get("meters", {}).get("estimated_diameter_min", ""),
                    "estimated_diameter_max_m": obj.get("estimated_diameter", {}).get("meters", {}).get("estimated_diameter_max", ""),
                    "is_potentially_hazardous_asteroid": obj.get("is_potentially_hazardous_asteroid", False),
                    "close_approach_date": approach.get("close_approach_date", ""),
                    "relative_velocity_kph": float(approach.get("relative_velocity", {}).get("kilometers_per_hour", 0)),
                    "miss_distance_km": float(approach.get("miss_distance", {}).get("kilometers", 0)),
                    "orbiting_body": approach.get("orbiting_body", ""),
                    "sentry_object": obj.get("is_sentry_object", False),
                })

        df = pd.DataFrame(asteroids)
        filename = f"neo_data_cleaned_{today.strftime('%Y_%m_%d')}.csv"
        csv_data = df.to_csv(index=False).encode("utf-8")

        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(csv_data, overwrite=True, content_settings=ContentSettings(content_type="text/csv"))

        logging.info(f"CSV uploaded to Azure Blob Storage: {filename}")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")


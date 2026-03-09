import os
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
BASE_PATH = "/data/bronze_files/lookup"
FILE_NAME = "taxi_zone_lookup.csv"


@data_loader
def load_taxi_zones(*args, **kwargs):

    os.makedirs(BASE_PATH, exist_ok=True)
    file_path = os.path.join(BASE_PATH, FILE_NAME)

    if os.path.exists(file_path):
        print(f"File already exists: {file_path}")
        return {"path": file_path, "status": "exists"}

    print("Downloading taxi zone lookup...")

    response = requests.get(URL)

    if response.status_code != 200:
        raise Exception(f"Download failed: HTTP {response.status_code}")

    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"Saved: {file_path}")

    return {"path": file_path, "status": "downloaded"}
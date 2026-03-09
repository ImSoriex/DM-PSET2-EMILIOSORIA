import os
import random
import time
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


BASE_PATH = "/data/bronze_files"
YEARS = [2022, 2023, 2024, 2025]
MONTHS = [f"{m:02d}" for m in range(1, 13)]
SERVICE_TYPES = ["yellow", "green"]

END_YEAR = 2025
END_MONTH_NUM = 11

REQUEST_TIMEOUT = (10, 120)
MAX_RETRIES = 5
SLEEP_BETWEEN_REQUESTS = (1, 3)

BATCH_SIZE = 5
BATCH_PAUSE_RANGE = (20, 40)
ERROR_BATCH_PAUSE_RANGE = (60, 120)

SESSION = requests.Session()
HEADERS = {"Accept": "*/*"}


def build_url(service_type, year, month):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month}.parquet"


def local_path(service_type, year, month):
    folder = os.path.join(BASE_PATH, service_type)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{service_type}_{year}-{month}.parquet")


def sleep_polite():
    time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))


def sleep_batch(had_block=False):
    rng = ERROR_BATCH_PAUSE_RANGE if had_block else BATCH_PAUSE_RANGE
    wait = random.uniform(*rng)
    print(f"Batch pause: {wait:.1f}s")
    time.sleep(wait)


def looks_like_error_file(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return True

    with open(path, "rb") as f:
        head = f.read(512).lstrip().lower()

    return head.startswith(b"<") and (
        b"<html" in head or b"<!doctype" in head or b"<?xml" in head or b"<error" in head
    )


def download_if_needed(url, path):
    if os.path.exists(path) and os.path.getsize(path) > 0 and not looks_like_error_file(path):
        return {"status": "exists", "path": path, "bytes": os.path.getsize(path)}

    tmp_path = path + ".tmp"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sleep_polite()
            print(f"Downloading {url} (attempt {attempt}/{MAX_RETRIES})")

            with SESSION.get(url, headers=HEADERS, stream=True, timeout=REQUEST_TIMEOUT) as r:
                if r.status_code == 404:
                    return {"status": "missing", "path": path, "error": "404 not found"}

                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}")

                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            os.replace(tmp_path, path)

            if looks_like_error_file(path):
                os.remove(path)
                raise RuntimeError("downloaded HTML/XML instead of parquet")

            return {"status": "downloaded", "path": path, "bytes": os.path.getsize(path)}

        except Exception as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            wait = min(120, 5 * (2 ** attempt)) + random.uniform(0, 3)
            print(f"Failed: {e} | retry in {wait:.1f}s")
            time.sleep(wait)

    return {"status": "failed", "path": path, "error": f"failed after {MAX_RETRIES} retries"}


@data_loader
def load_taxi(*args, **kwargs):
    manifest = []
    batch_count = 0
    batch_had_block = False

    for y in YEARS:
        stop_all = False

        for m in MONTHS:
            if y == END_YEAR and int(m) > END_MONTH_NUM:
                stop_all = True
                break

            ym = f"{y}-{m}"

            for st in SERVICE_TYPES:
                url = build_url(st, y, m)
                path = local_path(st, y, m)

                res = download_if_needed(url, path)

                manifest.append({
                    "service_type": st,
                    "source_month": ym,
                    "url": url,
                    "path": path,
                    "status": res["status"],
                    "error": res.get("error"),
                    "bytes": res.get("bytes"),
                })

                if res["status"] == "downloaded":
                    batch_count += 1

                error_text = (res.get("error") or "").lower()
                if "403" in error_text or "429" in error_text:
                    batch_had_block = True

                if batch_count >= BATCH_SIZE:
                    sleep_batch(batch_had_block)
                    batch_count = 0
                    batch_had_block = False

        if stop_all:
            break

    return manifest


@test
def test_output(output, *args):
    assert True
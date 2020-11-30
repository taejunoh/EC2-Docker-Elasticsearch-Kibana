import argparse
import sys
import os
import requests
from requests.auth import HTTPBasicAuth

from sodapy import Socrata

parser = argparse.ArgumentParser(description='Process data from project')
parser.add_argument('--page_size', type=int,
                    help = 'how many rows to get per page', required = True)
parser.add_argument('--num_pages', type=int,
                    help = 'how many pages to get in total')
args = parser.parse_args(sys.argv[1:])

DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

if __name__ == '__main__':
    try:
        resp = requests.put(
            f"{ES_HOST}/project",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            
            json = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "plate": {"type": "keyword"},
                        "state": {"type": "keyword"},
                        "license_type": {"type": "keyword"},
                        "summons_number": {"type": "keyword"},
                        "issue_date": {"type": "date", "format": "MM-dd-yyyy||dd-MM-yyyy"},
                        "violation_time": {"type": "keyword"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "keyword"},
                        "county": {"type": "keyword"},
                        "issuing_agency": {"type": "keyword"},
                    }
                },
            })
        resp.raise_for_status()
        print(resp.json())
    except Exception:
        print("Index already exists! Skipping")

    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )

    rows = client.get(DATASET_ID, limit=args.page_size)
    for row in rows:
        try:
            row["issue_date"] = row["issue_date"].replace("/", "-")
            row["fine_amount"] = float(row["fine_amount"])
            row["penalty_amount"] = float(row["penalty_amount"])
            row["interest_amount"] = float(row["interest_amount"])
            row["reduction_amount"] = float(row["reduction_amount"])
            row["payment_amount"] = float(row["payment_amount"])
            row["amount_due"] = float(row["amount_due"])
        except Exception as e:
            print(f"Error!: {e}, skpping row: {row}")
            continue
    
        try:
            resp = requests.post(
                f"{ES_HOST}/project/_doc",
                json=row,
                auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD)
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"Failed to insert in ES: {e}, skipping row: {row}")
            continue
        
        print(resp.json())

    # resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
    # print(resp.json())
"""
Minimal Debezium connector registration helper (optional).
Project now auto-registers the connector via docker-compose (debezium-init).
This script remains for manual use only.
"""
import json
import time
import requests

DEBEZIUM_URL = "http://localhost:8083"
CONNECTOR_CONFIG_FILE = "configs/debezium-connector.json"

if __name__ == "__main__":
    # Basic retry for availability
    for _ in range(30):
        try:
            r = requests.get(f"{DEBEZIUM_URL}/", timeout=2)
            if r.ok:
                break
        except Exception:
            pass
        time.sleep(2)

    with open(CONNECTOR_CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    resp = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(cfg),
    )

    # Treat 201 Created and 409 Conflict (already exists) as success
    if resp.status_code in (201, 200, 409):
        print("Connector registered (or already exists).")
    else:
        print(f"Failed to register connector: {resp.status_code} {resp.text}")

import json
import requests
from pathlib import Path

connect_url = "http://localhost:8083/connectors"

request = requests.get(connect_url)
if request.ok:
    connectors = request.json()
else:
    exit(f'failed to get connectors: {request.status_code} {request.text}')
for connector in connectors:
    request = requests.get(f"{connect_url}/{connector}")
    if request.ok:
        config = request.json()
        # cleaning
        if 'type' in config:
            del config['type']
        if 'tasks' in config:
            del config['tasks']
        config_file = Path("connect", f"{connector}.json")
        with config_file.open('w+') as f:
            json.dump(config, f, indent=4)
    else:
        exit(f"failed to get connector: {connector} - {request.status_code} {request.text}")


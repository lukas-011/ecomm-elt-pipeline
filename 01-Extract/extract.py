import json
import os
from pathlib import Path
from urllib import parse, request


def load_dotenv(path=".env"):
    env_path = Path(path)
    if not env_path.exists():
        return

    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"\'')
            if key and key not in os.environ:
                os.environ[key] = value

load_dotenv()

def get_access_token():
    """Fetch a fresh access token using client credentials. Valid for 24 hours."""
    store = os.getenv('SHOPIFY_STORE_URL')
    response = requests.post(
        f"https://{store}/admin/oauth/access_token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": os.getenv('SHOPIFY_CLIENT_ID'),
            "client_secret": os.getenv('SHOPIFY_CLIENT_SECRET'),
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]
import os
import requests
from datetime import datetime

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_ID = os.environ["NOTION_DATABASE_ID"].strip()
NOTION_VERSION = "2022-06-28"

def headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    title = f"Falco Bot Test — {now}"

    payload = {
        "parent": {"database_id": DB_ID},
        "properties": {
            "Property Name": {"title": [{"type": "text", "text": {"content": title}}]},
            "Source": {"select": {"name": "Bot Test"}},
            "Distress Type": {"select": {"name": "Test"}},
            "Falco Score": {"number": 1},
            "Status": {"select": {"name": "New"}},
        },
    }

    r = requests.post("https://api.notion.com/v1/pages", headers=headers(), json=payload, timeout=30)
    print("STATUS:", r.status_code)
    print("BODY:", r.text)
    r.raise_for_status()
    print("✅ Notion write successful.")

if __name__ == "__main__":
    main()

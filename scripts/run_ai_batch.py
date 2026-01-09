import json
import time
import csv
import os
import sys
from typing import Dict, Any

import requests

# =====================
# Config
# =====================

INPUT_CSV = "data/raw/project_Hugo_voor AI.csv"
PROMPT_FILE = "prompts/project33_prompt_v2.jsonl"
OUTPUT_JSONL = "data/processed/ai_output_300_2.jsonl"

API_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-5.2"

MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 0.5

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("ERROR: OPENAI_API_KEY not set")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# =====================
# Helpers
# =====================

def load_prompt_template(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_request(prompt_template: Dict[str, Any], omschrijving: str) -> Dict[str, Any]:
    return {
        "model": MODEL,
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": prompt_template["system"]["content"]
            },
            {
                "role": "user",
                "content": prompt_template["user"]["content"].replace(
                    "{{omschrijving}}", omschrijving.strip()
                )
            }
        ]
    }


def call_api(payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        timeout=60
    )
    response.raise_for_status()
    return response.json()


def extract_json_output(api_response: Dict[str, Any]) -> Dict[str, Any]:
    try:
        content = api_response["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception as e:
        raise ValueError(f"Invalid JSON output: {e}")


def validate_output_schema(data: Dict[str, Any]) -> None:
    required_fields = [
        "gedetecteerde_typen",
        "eindlabel",
        "onderzoeksstatus",
        "beleidsdoorlaat",
        "confidence",
        "reden"
    ]

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing field: {field}")

    if not isinstance(data["confidence"], (int, float)):
        raise ValueError("confidence is not numeric")

    if not 0.0 <= float(data["confidence"]) <= 1.0:
        raise ValueError("confidence out of range")


# =====================
# Main
# =====================

def main():
    prompt_template = load_prompt_template(PROMPT_FILE)

    os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

    with open(INPUT_CSV, newline="", encoding="cp1252") as csvfile, \
         open(OUTPUT_JSONL, "w", encoding="utf-8") as outfile:

        reader = csv.DictReader(csvfile, delimiter=";")

        if not reader.fieldnames:
            sys.exit("ERROR: CSV heeft geen header")

        if "rijksmonumentnummer" not in reader.fieldnames:
            sys.exit("ERROR: kolom 'rijksmonumentnummer' niet gevonden in CSV")

        if "omschrijving" not in reader.fieldnames:
            sys.exit("ERROR: kolom 'omschrijving' niet gevonden in CSV")

        for i, row in enumerate(reader, start=1):
            monument_id = row.get("rijksmonumentnummer")
            omschrijving = (row.get("omschrijving") or "").strip()

            if not monument_id or not omschrijving:
                print(f"[SKIP] Lege regel bij record {i}")
                continue

            payload = build_request(prompt_template, omschrijving)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    api_response = call_api(payload)
                    result = extract_json_output(api_response)
                    validate_output_schema(result)

                    output_record = {
                        "rijksmonumentnummer": monument_id,
                        "ai_result": result
                    }

                    outfile.write(
                        json.dumps(output_record, ensure_ascii=False) + "\n"
                    )
                    outfile.flush()

                    print(f"[OK] {monument_id}")
                    break

                except Exception as e:
                    print(f"[ERROR] {monument_id} (attempt {attempt}): {e}")
                    if attempt == MAX_RETRIES:
                        print(f"[FAIL] {monument_id} definitief overgeslagen")
                    else:
                        time.sleep(2)

            time.sleep(SLEEP_BETWEEN_REQUESTS)


if __name__ == "__main__":
    main()

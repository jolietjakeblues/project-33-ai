import csv
import json
import os
import time
import requests

# ======================
# CONFIG
# ======================

INPUT_CSV = "data/raw/project_Hugo_voor AI.csv"
OUTPUT_CSV = "data/processed/ai_signalen_voor_hugo.csv"

API_URL = "https://api.openai.com/v1/responses"
MODEL = "gpt-5.2"

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

# ======================
# PROMPT (signalering)
# ======================

SYSTEM_PROMPT = (
    "U ondersteunt een inhoudelijk expert bij het lezen van rijksmonumentenomschrijvingen.\n"
    "Uw taak is uitsluitend signalerend.\n"
    "U neemt geen beslissingen en kent geen labels toe.\n"
    "U markeert alleen expliciete formuleringen.\n"
    "Bij twijfel markeert u niets.\n"
    "U citeert altijd letterlijk uit de tekst.\n"
)

USER_PROMPT_TEMPLATE = """
Lees de onderstaande omschrijving.

Markeer uitsluitend expliciete tekstfragmenten die mogelijk vallen onder één of meer van deze vier typen.

1. Uitsluiting
2. Relatieve waardering
3. Constatering
4. Bescherming vanwege

Regels:
- Markeer alleen expliciete formuleringen.
- Bij twijfel: markeer niets.
- Meerdere fragmenten per type zijn toegestaan.
- Geef geen oordeel of uitleg.

Geef uitsluitend geldige JSON volgens dit schema:

{
  "heeft_uitsluiting": true | false,
  "heeft_relatieve_waardering": true | false,
  "heeft_constatering": true | false,
  "heeft_bescherming_vanwege": true | false,
  "fragmenten": {
    "uitsluiting": [],
    "relatieve_waardering": [],
    "constatering": [],
    "bescherming_vanwege": []
  }
}

Omschrijving:
\"\"\"{omschrijving}\"\"\"
"""

# ======================
# HELPERS
# ======================

def call_ai(omschrijving: str) -> dict:
    payload = {
        "model": MODEL,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(omschrijving=omschrijving)}
        ],
        "temperature": 0,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "ai_signalen",
                "schema": {
                    "type": "object",
                    "properties": {
                        "heeft_uitsluiting": {"type": "boolean"},
                        "heeft_relatieve_waardering": {"type": "boolean"},
                        "heeft_constatering": {"type": "boolean"},
                        "heeft_bescherming_vanwege": {"type": "boolean"},
                        "fragmenten": {
                            "type": "object",
                            "properties": {
                                "uitsluiting": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                                "relatieve_waardering": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                                "constatering": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                                "bescherming_vanwege": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                }
                            },
                            "required": [
                                "uitsluiting",
                                "relatieve_waardering",
                                "constatering",
                                "bescherming_vanwege"
                            ],
                            "additionalProperties": False
                        }
                    },
                    "required": [
                        "heeft_uitsluiting",
                        "heeft_relatieve_waardering",
                        "heeft_constatering",
                        "heeft_bescherming_vanwege",
                        "fragmenten"
                    ],
                    "additionalProperties": False
                }
            }
        }
    }

    response = requests.post(API_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json()["output_parsed"]




# ======================
# MAIN
# ======================

def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    with open(INPUT_CSV, newline="", encoding="cp1252") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile, delimiter=";")

        fieldnames = [
            "rijksmonumentnummer",
            "hoofdcategorie",
            "adres",
            "woonplaatsnaam",
            "ai_heeft_uitsluiting",
            "ai_heeft_relatieve_waardering",
            "ai_heeft_constatering",
            "ai_heeft_bescherming_vanwege",
            "ai_fragment_uitsluiting",
            "ai_fragment_relatieve_waardering",
            "ai_fragment_constatering",
            "ai_fragment_bescherming_vanwege",
            "omschrijving",
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for i, row in enumerate(reader, start=1):
            omschrijving = row.get("omschrijving", "").strip()
            if not omschrijving:
                continue

            try:
                ai_result = call_ai(omschrijving)
            except Exception as e:
                print(f"[ERROR] {row.get('rijksmonumentnummer')} – {e}")
                continue

            writer.writerow({
                "rijksmonumentnummer": row.get("rijksmonumentnummer"),
                "hoofdcategorie": row.get("hoofdcategorie"),
                "adres": row.get("adres"),
                "woonplaatsnaam": row.get("woonplaatsnaam"),
                "ai_heeft_uitsluiting": ai_result.get("heeft_uitsluiting"),
                "ai_heeft_relatieve_waardering": ai_result.get("heeft_relatieve_waardering"),
                "ai_heeft_constatering": ai_result.get("heeft_constatering"),
                "ai_heeft_bescherming_vanwege": ai_result.get("heeft_bescherming_vanwege"),
                "ai_fragment_uitsluiting": join_fragments(ai_result["fragmenten"]["uitsluiting"]),
                "ai_fragment_relatieve_waardering": join_fragments(ai_result["fragmenten"]["relatieve_waardering"]),
                "ai_fragment_constatering": join_fragments(ai_result["fragmenten"]["constatering"]),
                "ai_fragment_bescherming_vanwege": join_fragments(ai_result["fragmenten"]["bescherming_vanwege"]),
                "omschrijving": omschrijving,
            })

            if i % 10 == 0:
                print(f"[OK] verwerkt: {i}")

            time.sleep(0.5)  # bewust rustig

    print(f"[KLAAR] CSV geschreven: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

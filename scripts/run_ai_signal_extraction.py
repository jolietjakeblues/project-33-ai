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
MODEL = "gpt-4o-2024-08-06"  # json_schema supported (Structured Outputs)

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
    "Geef uitsluitend JSON volgens het gevraagde schema.\n"
)

USER_PROMPT_TEMPLATE = """
Lees de onderstaande omschrijving.

Markeer uitsluitend expliciete tekstfragmenten die mogelijk vallen onder één of meer van deze vier typen:
1. Uitsluiting
2. Relatieve waardering
3. Constatering
4. Bescherming vanwege

Regels:
- Markeer alleen expliciete formuleringen.
- Bij twijfel: markeer niets.
- Meerdere fragmenten per type zijn toegestaan.
- Geef geen oordeel of uitleg.

Omschrijving:
\"\"\"{omschrijving}\"\"\"
"""

# ======================
# HELPERS
# ======================

def join_fragments(fragments):
    if not fragments:
        return ""
    return " | ".join(fragments)

def extract_output_text(response_json: dict) -> str:
    texts = []
    for item in response_json.get("output", []):
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                texts.append(c.get("text", ""))
    return "\n".join(texts).strip()

def call_ai(omschrijving: str) -> dict:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "heeft_uitsluiting": {"type": "boolean"},
            "heeft_relatieve_waardering": {"type": "boolean"},
            "heeft_constatering": {"type": "boolean"},
            "heeft_bescherming_vanwege": {"type": "boolean"},
            "fragmenten": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "uitsluiting": {"type": "array", "items": {"type": "string"}},
                    "relatieve_waardering": {"type": "array", "items": {"type": "string"}},
                    "constatering": {"type": "array", "items": {"type": "string"}},
                    "bescherming_vanwege": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["uitsluiting", "relatieve_waardering", "constatering", "bescherming_vanwege"],
            },
        },
        "required": [
            "heeft_uitsluiting",
            "heeft_relatieve_waardering",
            "heeft_constatering",
            "heeft_bescherming_vanwege",
            "fragmenten",
        ],
    }

    payload = {
        "model": MODEL,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(omschrijving=omschrijving)},
        ],
        "temperature": 0,
        # Responses API: Structured Outputs live under text.format
        "text": {
            "format": {
                "type": "json_schema",
                "name": "ai_signalen",
                "strict": True,
                "schema": schema,
            }
        },
    }

    r = requests.post(API_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    # Meestal staat het al als valide JSON in output_text. Pak die en parse.
    out = extract_output_text(data)
    return json.loads(out)

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

        written = 0

        for i, row in enumerate(reader, start=1):
            omschrijving = (row.get("omschrijving") or "").strip()
            if not omschrijving:
                continue

            try:
                ai_result = call_ai(omschrijving)
            except Exception as e:
                print(f"[ERROR] {row.get('rijksmonumentnummer')} – {repr(e)}")
                continue

            writer.writerow({
                "rijksmonumentnummer": row.get("rijksmonumentnummer"),
                "hoofdcategorie": row.get("hoofdcategorie"),
                "adres": row.get("adres"),
                "woonplaatsnaam": row.get("woonplaatsnaam"),
                "ai_heeft_uitsluiting": ai_result.get("heeft_uitsluiting", False),
                "ai_heeft_relatieve_waardering": ai_result.get("heeft_relatieve_waardering", False),
                "ai_heeft_constatering": ai_result.get("heeft_constatering", False),
                "ai_heeft_bescherming_vanwege": ai_result.get("heeft_bescherming_vanwege", False),
                "ai_fragment_uitsluiting": join_fragments(ai_result["fragmenten"]["uitsluiting"]),
                "ai_fragment_relatieve_waardering": join_fragments(ai_result["fragmenten"]["relatieve_waardering"]),
                "ai_fragment_constatering": join_fragments(ai_result["fragmenten"]["constatering"]),
                "ai_fragment_bescherming_vanwege": join_fragments(ai_result["fragmenten"]["bescherming_vanwege"]),
                "omschrijving": omschrijving,
            })

            written += 1
            if i % 10 == 0:
                print(f"[OK] gelezen: {i} | geschreven: {written}")

            time.sleep(0.2)

    print(f"[KLAAR] CSV geschreven: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

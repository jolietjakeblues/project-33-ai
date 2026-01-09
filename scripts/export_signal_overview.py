import json
import csv
import os

INPUT_JSONL = "data/processed/ai_output_300.jsonl"
OUTPUT_CSV = "data/processed/ai_signal_overview_300.csv"

FIELDNAMES = [
    "rijksmonumentnummer",
    "heeft_uitsluiting",
    "heeft_relatieve_waardering",
    "heeft_constatering",
    "heeft_bescherming_vanwege",
    "eindlabel",
    "confidence",
]

def has_any(dct, key):
    value = dct.get(key, [])
    return bool(value and isinstance(value, list) and len(value) > 0)

def main():
    if not os.path.exists(INPUT_JSONL):
        raise FileNotFoundError(f"Input not found: {INPUT_JSONL}")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    with open(INPUT_JSONL, encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

        writer = csv.DictWriter(outfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for line in infile:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            ai = record.get("ai_result", {})
            detected = ai.get("gedetecteerde_typen", {})

            writer.writerow({
                "rijksmonumentnummer": record.get("rijksmonumentnummer", ""),
                "heeft_uitsluiting": has_any(detected, "uitsluiting"),
                "heeft_relatieve_waardering": has_any(detected, "relatieve_waardering"),
                "heeft_constatering": has_any(detected, "constatering"),
                "heeft_bescherming_vanwege": has_any(detected, "bescherming_vanwege"),
                "eindlabel": ai.get("eindlabel", ""),
                "confidence": ai.get("confidence", ""),
            })

    print(f"[OK] Overzicht geschreven: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

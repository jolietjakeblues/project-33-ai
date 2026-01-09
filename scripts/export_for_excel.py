import json
import csv
import os

INPUT_JSONL = "data/processed/ai_output_300.jsonl"
OUTPUT_CSV = "data/processed/ai_output_300_for_excel.csv"

FIELDNAMES = [
    "rijksmonumentnummer",
    "eindlabel",
    "onderzoeksstatus",
    "beleidsdoorlaat",
    "confidence",
    "reden",
    "uitsluiting_fragments",
    "relatieve_waardering_fragments",
    "constatering_fragments",
    "vanwege_fragments",
]

def join_fragments(fragments):
    if not fragments:
        return ""
    # Excel-vriendelijk scheidingsteken
    return " | ".join(str(x).strip() for x in fragments if str(x).strip())

def safe_get_list(dct, key):
    value = dct.get(key, [])
    return value if isinstance(value, list) else []

def main():
    if not os.path.exists(INPUT_JSONL):
        raise FileNotFoundError(f"Input not found: {INPUT_JSONL}")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    with open(INPUT_JSONL, encoding="utf-8") as infile, \
         open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

        writer = csv.DictWriter(outfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for line_no, line in enumerate(infile, start=1):
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            monument_id = record.get("rijksmonumentnummer", "")
            ai = record.get("ai_result", {})
            detected = ai.get("gedetecteerde_typen", {})

            row = {
                "rijksmonumentnummer": monument_id,
                "eindlabel": ai.get("eindlabel", ""),
                "onderzoeksstatus": ai.get("onderzoeksstatus", ""),
                "beleidsdoorlaat": ai.get("beleidsdoorlaat", False),
                "confidence": ai.get("confidence", ""),
                "reden": ai.get("reden", ""),
                "uitsluiting_fragments": join_fragments(safe_get_list(detected, "uitsluiting")),
                "relatieve_waardering_fragments": join_fragments(safe_get_list(detected, "relatieve_waardering")),
                "constatering_fragments": join_fragments(safe_get_list(detected, "constatering")),
                "vanwege_fragments": join_fragments(safe_get_list(detected, "vanwege")),
            }

            writer.writerow(row)

    print(f"[OK] Export geschreven: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

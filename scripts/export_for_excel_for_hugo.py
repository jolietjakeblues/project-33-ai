import json
import csv
import os

INPUT_SOURCE_CSV = "data/raw/project_Hugo_voor AI.csv"
INPUT_JSONL = "data/processed/ai_output_300_2.jsonl"
OUTPUT_CSV = "data/processed/ai_vs_hugo_300_for_excel.csv"

ID_COL = "rijksmonumentnummer"
HUGO_LABEL_COL = "Type beschrijving"   # pas aan als de kolom anders heet

FIELDNAMES = [
    ID_COL,
    "hugo_label",
    "ai_label",
    "match",
    "confidence",
    "onderzoeksstatus",
    "beleidsdoorlaat",
    "reden",
    "uitsluiting_fragments",
    "relatieve_waardering_fragments",
    "constatering_fragments",
    "vanwege_fragments",
]

def join_fragments(fragments):
    if not fragments:
        return ""
    return " | ".join(str(x).strip() for x in fragments if str(x).strip())

def safe_get_list(dct, key):
    value = dct.get(key, [])
    return value if isinstance(value, list) else []

def normalize_label(label: str) -> str:
    if label is None:
        return ""
    return str(label).strip().lower()

def load_hugo_labels() -> dict:
    if not os.path.exists(INPUT_SOURCE_CSV):
        raise FileNotFoundError(f"Input not found: {INPUT_SOURCE_CSV}")

    labels = {}
    with open(INPUT_SOURCE_CSV, newline="", encoding="cp1252") as f:
        reader = csv.DictReader(f, delimiter=";")

        if ID_COL not in reader.fieldnames:
            raise ValueError(f"Kolom '{ID_COL}' niet gevonden in bron-CSV")
        if HUGO_LABEL_COL not in reader.fieldnames:
            raise ValueError(f"Kolom '{HUGO_LABEL_COL}' niet gevonden in bron-CSV")

        for row in reader:
            rid = str(row.get(ID_COL, "")).strip()
            hugo = normalize_label(row.get(HUGO_LABEL_COL, ""))
            if rid:
                labels[rid] = hugo

    return labels

def main():
    hugo_by_id = load_hugo_labels()

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
            rid = str(record.get(ID_COL, "")).strip()
            ai = record.get("ai_result", {})
            detected = ai.get("gedetecteerde_typen", {})

            ai_label = normalize_label(ai.get("eindlabel", ""))
            hugo_label = hugo_by_id.get(rid, "")

            writer.writerow({
                ID_COL: rid,
                "hugo_label": hugo_label,
                "ai_label": ai_label,
                "match": (ai_label == hugo_label) if (ai_label and hugo_label) else False,
                "confidence": ai.get("confidence", ""),
                "onderzoeksstatus": ai.get("onderzoeksstatus", ""),
                "beleidsdoorlaat": ai.get("beleidsdoorlaat", False),
                "reden": ai.get("reden", ""),
                "uitsluiting_fragments": join_fragments(safe_get_list(detected, "uitsluiting")),
                "relatieve_waardering_fragments": join_fragments(safe_get_list(detected, "relatieve_waardering")),
                "constatering_fragments": join_fragments(safe_get_list(detected, "constatering")),
                "vanwege_fragments": join_fragments(safe_get_list(detected, "vanwege")),
            })

    print(f"[OK] Export geschreven: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

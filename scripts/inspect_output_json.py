import json
import pandas as pd

INPUT = "data/processed/ai_output_300_2.jsonl"

rows = []

with open(INPUT, encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        ai = record["ai_result"]

        rows.append({
            "rijksmonumentnummer": record["rijksmonumentnummer"],
            "eindlabel": ai["eindlabel"],
            "onderzoeksstatus": ai["onderzoeksstatus"],
            "beleidsdoorlaat": ai["beleidsdoorlaat"],
            "confidence": ai["confidence"],
            "reden": ai["reden"]
        })

df = pd.DataFrame(rows)
print(df.head())

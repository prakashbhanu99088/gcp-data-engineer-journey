import json
import csv

INPUT_JSON = "raw/random_users.json"
OUTPUT_CSV = "data/random_users.csv"

# 1) Read JSON
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)

people = data["results"]

# 2) Choose columns for CSV (flat schema)
fieldnames = ["first_name", "last_name", "country", "email"]

# 3) Write CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for person in people:
        row = {
            "first_name": person["name"]["first"],
            "last_name": person["name"]["last"],
            "country": person["location"]["country"],
            "email": person["email"],
        }
        writer.writerow(row)

print(f"Saved {len(people)} rows to {OUTPUT_CSV}")

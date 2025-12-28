import json

FILE_PATH = "raw/random_users.json"

with open(FILE_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

people = data["results"]

print("Total people:", len(people))

# Print 3 sample records (name + country)
for i, person in enumerate(people[:3], start=1):
    first = person["name"]["first"]
    last = person["name"]["last"]
    country = person["location"]["country"]
    print(f"{i}. {first} {last} - {country}")
country_counts = {}

for person in people:
    country = person["location"]["country"]
    country_counts[country] = country_counts.get(country, 0) + 1

print("Country counts:", country_counts)

import json
import urllib.request

api_url="https://randomuser.me/api/?results=5"

with urllib.request.urlopen(api_url) as response:
	data=response.read().decode("utf-8")
 
json_data=json.loads(data)

with open("raw/random_users.json","w",encodi ng="utf-8") as f:
	json.dump(json_data, f, indent=2)

print("API data saved to raw/random_users.json") 
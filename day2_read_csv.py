import csv

total = 0
count = 0

with open("data/sales.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        total += int(row["amount"])
        count += 1

avg = total / count if count else 0

print("Rows:", count)
print("Total sales:", total)
print("Average sale:", avg)

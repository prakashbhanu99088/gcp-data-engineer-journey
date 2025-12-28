def summarize(values):
    total = sum(values)
    count = len(values)
    avg = total / count if count else 0
    return total, count, avg

sales = [120, 90, 150, 200, 175]

total, count, avg = summarize(sales)

print("Rows:", count)
print("Total:", total)
print("Average:", avg)

import csv
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "dedb",
    "user": "deuser",
    "password": "depass",
}

CSV_PATH = "data/random_users.csv"

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Clear table so re-running doesn't duplicate rows (simple beginner approach)
    cur.execute("TRUNCATE TABLE users_staging;")

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["first_name"], r["last_name"], r["country"], r["email"]) for r in reader]

    cur.executemany(
        "INSERT INTO users_staging (first_name, last_name, country, email) VALUES (%s, %s, %s, %s);",
        rows
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM users_staging;")
    print("Loaded rows:", cur.fetchone()[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()

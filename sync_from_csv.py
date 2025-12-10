import os
import csv
import sys

CSV_PATH = "players.csv"

def main():
    print("=== CSV DEBUG ===")
    print("Working dir:", os.getcwd())
    print("CSV exists?:", os.path.exists(CSV_PATH))

    if not os.path.exists(CSV_PATH):
        print("❌ players.csv not found")
        sys.exit(1)

    # Lire brut
    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()

    print("File size (chars):", len(raw))
    lines = raw.splitlines()
    print("Line count:", len(lines))

    # Afficher les 5 premières lignes brutes
    print("\n--- First raw lines ---")
    for i, l in enumerate(lines[:5]):
        print(f"{i}: {repr(l)}")

    # Parser avec csv
    print("\n--- csv.reader preview ---")
    reader = csv.reader(lines, delimiter=",")
    rows = list(reader)
    print("Row count (csv):", len(rows))
    if rows:
        print("Header row:", rows[0])
    if len(rows) > 1:
        print("First data row:", rows[1])

    # DictReader
    print("\n--- csv.DictReader preview ---")
    dict_reader = csv.DictReader(lines, delimiter=",")
    print("Fieldnames:", dict_reader.fieldnames)
    count = 0
    first_row = None
    for row in dict_reader:
        if any(v.strip() for v in row.values()):
            count += 1
            if first_row is None:
                first_row = row
    print("Non-empty rows (DictReader):", count)
    if first_row:
        print("First DictReader row:", first_row)

    print("=== END CSV DEBUG ===")

if __name__ == "__main__":
    main()

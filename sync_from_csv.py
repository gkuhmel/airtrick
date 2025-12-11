import os
import csv
import sys
import json
import requests

print("=== SCRIPT STARTED ===")

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = "Players"

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

CSV_PATH = "players.csv"


# ---------------------------------------------------
# UTILS
# ---------------------------------------------------

def fail(msg):
    print("❌ FATAL ERROR:", msg)
    sys.exit(1)


def normalize(s: str) -> str:
    """Nettoie BOM + espaces + insécables."""
    if s is None:
        return ""
    return (
        s.replace("\ufeff", "")    # BOM UTF-8
         .replace("\u00A0", " ")  # espace insécable
         .replace("\u202F", " ")  # narrow no-break space
         .strip()
    )


# ---------------------------------------------------
# CSV LOADING
# ---------------------------------------------------

def load_csv_players():
    print("=== CSV LOADING START ===")
    print("CSV path:", CSV_PATH)

    if not os.path.exists(CSV_PATH):
        fail("CSV file not found")

    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()

    raw = raw.replace("\u00A0", " ").replace("\u202F", " ")
    lines = raw.splitlines()

    print(f"CSV lines detected: {len(lines)}")

    reader = csv.DictReader(lines, delimiter=",")

    raw_headers = reader.fieldnames or []
    normalized_headers = [normalize(h) for h in raw_headers]

    print("🔎 Headers normalized:", normalized_headers)

    # Mapping brut → normalisé
    fieldmap = dict(zip(raw_headers, normalized_headers))

    # Trouver colonne ID
    pid_key = None
    for h in normalized_headers:
        if "id du joueur" in h.lower():
            pid_key = h
            break

    if not pid_key:
        fail("❌ Impossible de trouver la colonne 'ID du joueur'")

    print("🧩 PlayerID column detected:", pid_key)

    players = []
    for raw_row in reader:
        row = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}

        pid = row.get(pid_key)
        if pid and pid.isdigit():
            players.append(row)

    print(f"✅ Players parsed: {len(players)}")

    if players:
        print("👀 Example player:", players[0].get("Nom"), "-", players[0].get(pid_key))

    print("=== CSV LOADING END ===")
    return players


# ---------------------------------------------------
# AIRTABLE LOAD EXISTING RECORDS
# ---------------------------------------------------

def load_airtable_existing():
    print("=== AIRTABLE LOAD EXISTING ===")
    print("Url:", AIRTABLE_URL)

    existing = {}
    params = {"pageSize": 100}

    while True:
        resp = requests.get(AIRTABLE_URL, headers=HEADERS, params=params)
        print("GET status:", resp.status_code)

        try:
            data = resp.json()
        except:
            fail("❌ Invalid JSON from Airtable: " + resp.text)

        print("GET response snippet:", json.dumps(data, indent=2)[:300])

        if resp.status_code != 200:
            fail("❌ Airtable read error: " + resp.text)

        for rec in data.get("records", []):
            pid = rec["fields"].get("PlayerID")
            if pid:
                existing[str(pid)] = rec["id"]

        if "offset" not in data:
            break

        params["offset"] = data["offset"]

    print(f"Existing players stored: {len(existing)}")
    return existing


# ---------------------------------------------------
# BUILD FIELDS FOR AIRTABLE
# ---------------------------------------------------

def extract_skill(row):
    def to_int(x):
        try: return int(x)
        except: return 0

    return to_int(row.get("Buteur")), to_int(row.get("Passe"))


def build_fields(row):
    return {
        "PlayerID": normalize(row.get("ID du joueur")),
        "Name": normalize(row.get("Nom")),
        "AgeYears": int(normalize(row.get("Âge")) or 0),
        "AgeDays": int(normalize(row.get("Jours")) or 0),
        "Specialty": normalize(row.get("Spécialité")),
        "Salary": int(normalize(row.get("Salaire")) or 0),
        "Form": int(normalize(row.get("Forme")) or 0),
        "Stamina": int(normalize(row.get("Endurance")) or 0),
        "SkillMain": extract_skill(row)[0],
        "SkillSecondary": extract_skill(row)[1],
        "Position": normalize(row.get("Poste au dernier match")),
        "LastSkillUp": normalize(row.get("Date du dernier match")),
    }


# ---------------------------------------------------
# UPSERT
# ---------------------------------------------------

def upsert(players, existing_index):
    print("=== UPSERT START ===")
    print("Total players to sync:", len(players))

    for row in players:
        pid = normalize(row.get("ID du joueur"))
        fields = build_fields(row)

        print("\n➡️ Processing player:", fields["Name"])
        print("PlayerID =", pid)
        print("Fields sent =", json.dumps(fields, indent=2))

        # UPDATE
        if pid in existing_index:
            rec_id = existing_index[pid]
            print("🔁 UPDATE record:", rec_id)

            resp = requests.patch(f"{AIRTABLE_URL}/{rec_id}", headers=HEADERS, json={"fields": fields})

            print("PATCH status:", resp.status_code)
            print("PATCH response:", resp.text)

            continue

        # CREATE
        print("✨ CREATE new record")
        resp = requests.post(AIRTABLE_URL, headers=HEADERS, json={"fields": fields})

        print("POST status:", resp.status_code)
        print("POST response:", resp.text)

    print("=== UPSERT END ===")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("=== MAIN START ===")

    if not AIRTABLE_API_KEY:
        fail("AIRTABLE_API_KEY missing")
    if not AIRTABLE_BASE_ID:
        fail("AIRTABLE_BASE_ID missing")

    print("Airtable Base:", AIRTABLE_BASE_ID)
    print("Airtable Table:", AIRTABLE_TABLE)

    players = load_csv_players()
    existing = load_airtable_existing()
    upsert(players, existing)

    print("=== MAIN FINISHED ===")


# Always call main()
if __name__ == "__main__":
    print("=== CALLING MAIN ===")
    main()

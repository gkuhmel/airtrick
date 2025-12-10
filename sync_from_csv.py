import os
import csv
import sys
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
    print("❌", msg)
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

    if not os.path.exists(CSV_PATH):
        fail(f"CSV file not found: {CSV_PATH}")

    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()

    raw = raw.replace("\u00A0", " ").replace("\u202F", " ")
    lines = raw.splitlines()

    print(f"CSV lines: {len(lines)}")

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
        fail("Impossible de trouver la colonne ID du joueur")

    print("🧩 PlayerID column detected:", pid_key)

    players = []
    for raw_row in reader:
        row = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}

        pid = row.get(pid_key)
        if pid and pid.isdigit():
            players.append(row)

    print(f"✅ Players parsed: {len(players)}")

    if players:
        print("👀 First player:", players[0].get("Nom"), "-", players[0].get(pid_key))

    print("=== CSV LOADING END ===")
    return players


# ---------------------------------------------------
# AIRTABLE READ EXISTING
# ---------------------------------------------------

def load_airtable_existing():
    print("=== AIRTABLE LOAD EXISTING ===")

    existing = {}
    params = {"pageSize": 100}

    while True:
        resp = requests.get(AIRTABLE_URL, headers=HEADERS, params=params)
        if resp.status_code != 200:
            fail(f"Airtable read error: {resp.text}")

        data = resp.json()
        for rec in data.get("records", []):
            pid = rec["fields"].get("PlayerID")
            if pid:
                existing[str(pid)] = rec["id"]

        if "offset" not in data:
            break

        params["offset"] = data["offset"]

    print(f"Existing players in Airtable: {len(existing)}")
    return existing


# ---------------------------------------------------
# BUILD FIELDS
# ---------------------------------------------------

def extract_skill(row):
    def to_int(x):
        try: return int(x)
        except: return 0

    return to_int(row.get("Buteur")), to_int(row.get("Passe"))


def build_fields(row):
    pid = normalize(row.get("ID du joueur"))
    s_main, s_sec = extract_skill(row)

    return {
        "PlayerID": pid,
        "Name": normalize(row.get("Nom")),
        "AgeYears": int(normalize(row.get("Âge")) or 0),
        "AgeDays": int(normalize(row.get("Jours")) or 0),
        "Specialty": normalize(row.get("Spécialité")),
        "Salary": int(normalize(row.get("Salaire")) or 0),
        "Form": int(normalize(row.get("Forme")) or 0),
        "Stamina": int(normalize(row.get("Endurance")) or 0),
        "SkillMain": s_main,
        "SkillSecondary": s_sec,
        "Position": normalize(row.get("Poste au dernier match")),
        "LastSkillUp": normalize(row.get("Date du dernier match")),
    }


# ---------------------------------------------------
# UPSERT
# ---------------------------------------------------

def upsert(players, existing_index):
    print("=== UPSERT START ===")

    for row in players:
        pid = normalize(row.get("ID du joueur"))
        fields = build_fields(row)

        if pid in existing_index:
            rec_id = existing_index[pid]
            resp = requests.patch(f"{AIRTABLE_URL}/{rec_id}", json={"fields": fields}, headers=HEADERS)
            print(f"🔁 Updated {fields['Name']}")
        else:
            resp = requests.post(AIRTABLE_URL, json={"fields": fields}, headers=HEADERS)
            print(f"✨ Created {fields['Name']}")

    print("=== UPSERT END ===")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("=== MAIN STARTED ===")

    if not AIRTABLE_API_KEY:
        fail("AIRTABLE_API_KEY missing")
    if not AIRTABLE_BASE_ID:
        fail("AIRTABLE_BASE_ID missing")

    print("Airtable Base:", AIRTABLE_BASE_ID)

    players = load_csv_players()
    existing = load_airtable_existing()
    upsert(players, existing)

    print("=== MAIN FINISHED ===")


# Always call main()
if __name__ == "__main__":
    print("=== CALLING MAIN ===")
    main()
s
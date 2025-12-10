import os
import csv
import sys
import requests

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = "Players"  # ta table renommée dans Airtable

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

airtable_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}


def fail(msg):
    print("❌", msg)
    sys.exit(1)


def normalize(s: str) -> str:
    """Nettoie les chaînes : BOM, insécables, espaces autour."""
    if s is None:
        return ""
    return (
        s.replace("\ufeff", "")   # BOM éventuel
         .replace("\u00A0", " ") # espace insécable classique
         .replace("\u202F", " ") # narrow no-break space (cas fréquent)
         .strip()
    )


# ---------------------------------------------------
# CSV LOADING – ultra tolérant
# ---------------------------------------------------

def load_csv_players(csv_path="players.csv"):
    print(f"⏬ Loading CSV file: {csv_path}")

    if not os.path.exists(csv_path):
        fail(f"CSV file not found: {csv_path}")

    # Lire tout en nettoyant BOM et insécables globalement
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()
    raw = raw.replace("\u00A0", " ").replace("\u202F", " ")

    lines = raw.splitlines()
    if not lines:
        fail("CSV file is empty")

    # DictReader avec séparateur forcé à la virgule
    reader = csv.DictReader(lines, delimiter=",")

    # Normalisation des noms de colonnes
    raw_fieldnames = reader.fieldnames or []
    normalized_fieldnames = [normalize(col) for col in raw_fieldnames]
    print("🔎 Debug header normalized:", normalized_fieldnames)

    # mapping "nom brut" -> "nom normalisé"
    fieldmap = dict(zip(raw_fieldnames, normalized_fieldnames))

    # Trouver la colonne ID joueur de manière robuste
    pid_key = None
    for name in normalized_fieldnames:
        if "id du joueur" in name.lower():
            pid_key = name
            break

    if not pid_key:
        fail("Impossible de trouver la colonne 'ID du joueur' dans le header normalisé")

    print("🧩 Detected PlayerID column:", repr(pid_key))

    players = []

    for raw_row in reader:
        # Normaliser toutes les colonnes
        row = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}

        pid = row.get(pid_key)
        if pid:
            players.append(row)

    print(f"✅ {len(players)} players found in CSV")

    if players:
        first = players[0]
        print("👀 Example player from CSV:",
              first.get("Nom"), "- ID:", first.get(pid_key))

    return players


# ---------------------------------------------------
# AIRTABLE LOAD
# ---------------------------------------------------

def load_airtable_players():
    print("⏬ Loading existing players from Airtable...")
    index = {}
    params = {"pageSize": 100}

    while True:
        resp = requests.get(AIRTABLE_URL, headers=airtable_headers, params=params)
        if resp.status_code != 200:
            fail(f"Airtable read error: {resp.text}")

        data = resp.json()
        for rec in data.get("records", []):
            pid = rec["fields"].get("PlayerID")
            if pid:
                index[str(pid)] = rec["id"]

        if "offset" not in data:
            break

        params["offset"] = data["offset"]

    print(f"✅ {len(index)} players currently stored in Airtable")
    return index


# ---------------------------------------------------
# SKILLS – entraînement buteur
# ---------------------------------------------------

def extract_skill(row):
    def to_int(x):
        try:
            return int(x)
        except:
            return 0

    main = to_int(row.get("Buteur"))
    secondary = to_int(row.get("Passe"))
    return main, secondary


# ---------------------------------------------------
# BUILD FIELDS FOR AIRTABLE
# ---------------------------------------------------

def build_fields(row):
    pid = normalize(row.get("ID du joueur"))  # avec la normalisation, ça marche

    skill_main, skill_secondary = extract_skill(row)

    fields = {
        "PlayerID": pid,
        "Name": normalize(row.get("Nom")),
        "AgeYears": int(normalize(row.get("Âge")) or 0),
        "AgeDays": int(normalize(row.get("Jours")) or 0),
        "Specialty": normalize(row.get("Spécialité")),
        "Salary": int(normalize(row.get("Salaire")) or 0),  # "Salaire " normalisé
        "Form": int(normalize(row.get("Forme")) or 0),
        "Stamina": int(normalize(row.get("Endurance")) or 0),
        "SkillMain": skill_main,
        "SkillSecondary": skill_secondary,
        "Position": normalize(row.get("Poste au dernier match")),
        "LastSkillUp": normalize(row.get("Date du dernier match")),
    }

    return fields


# ---------------------------------------------------
# UPSERT
# ---------------------------------------------------

def upsert(csv_players, airtable_index):
    csv_ids = set()

    for row in csv_players:
        player_id = normalize(row.get("ID du joueur"))
        csv_ids.add(player_id)

        fields = build_fields(row)

        if player_id in airtable_index:
            rec_id = airtable_index[player_id]
            resp = requests.patch(
                f"{AIRTABLE_URL}/{rec_id}",
                headers=airtable_headers,
                json={"fields": fields},
            )
            if resp.status_code not in (200, 201):
                fail(f"Update error for {fields['Name']}: {resp.text}")
            print(f"🔁 Updated {fields['Name']}")
        else:
            resp = requests.post(
                AIRTABLE_URL,
                headers=airtable_headers,
                json={"fields": fields},
            )
            if resp.status_code not in (200, 201):
                fail(f"Create error for {fields['Name']}: {resp.text}")
            print(f"✨ Created {fields['Name']}")

    return csv_ids


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        fail("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")

    print("🔍 DEBUG Airtable config:")
    print("BASE_ID:", repr(AIRTABLE_BASE_ID))
    print("TABLE  :", repr(AIRTABLE_TABLE))
    print("URL    :", AIRTABLE_URL)
    print("------")

    csv_players = load_csv_players()
    airtable_index = load_airtable_players()
    csv_ids = upsert(csv_players, airtable_index)

    print("✅ Sync complete.")


if __name__ == "__main__":
    main()

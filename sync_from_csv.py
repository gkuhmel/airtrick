import os
import csv
import sys
import requests

# -------------------------------------
# CONFIG
# -------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = "Players"

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

airtable_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

TRAINING_TYPE = "Buteur"  # ton entraînement principal


# -------------------------------------
# UTILS
# -------------------------------------

def fail(msg):
    print("❌", msg)
    sys.exit(1)


def normalize(s):
    """Nettoie les chaînes :
    - supprime BOM
    - remplace les insécables
    - supprime les espaces autour
    """
    if s is None:
        return ""
    return (
        s.replace("\ufeff", "")   # BOM UTF-8
         .replace("\u00A0", " ") # espace insécable
         .strip()
    )


# -------------------------------------
# CSV LOADING (ROBUST)
# -------------------------------------

def load_csv_players(csv_path="players.csv"):
    print(f"⏬ Loading CSV file: {csv_path}")

    if not os.path.exists(csv_path):
        fail(f"CSV file not found: {csv_path}")

    # Lire tout le fichier et nettoyer BOM & insécables globalement
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        raw = f.read().replace("\u00A0", " ")

    lines = raw.splitlines()

    # Détecter automatiquement le séparateur
    try:
        dialect = csv.Sniffer().sniff("\n".join(lines[:5]))
    except:
        dialect = csv.excel
        dialect.delimiter = ','  # fallback

    reader = csv.DictReader(lines, dialect=dialect)

    # Normaliser les colonnes
    fieldmap = {col: normalize(col) for col in reader.fieldnames}

    players = []

    for raw_row in reader:
        # Normaliser ligne
        row = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}

        pid = row.get("ID du joueur") or row.get("ID du joueur ") or row.get("ID du joueur")
        if pid and pid != "":
            players.append(row)

    print(f"✅ {len(players)} players found in CSV")
    return players


# -------------------------------------
# AIRTABLE LOAD
# -------------------------------------

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


# -------------------------------------
# SKILL extraction for training BUTEUR
# -------------------------------------

def extract_skill(row):
    def to_int(x):
        try:
            return int(x)
        except:
            return 0

    # Buteur = skill principale
    main = to_int(row.get("Buteur"))
    secondary = to_int(row.get("Passe"))
    return main, secondary


# -------------------------------------
# BUILD FIELDS
# -------------------------------------

def build_fields(row):
    pid = normalize(row.get("ID du joueur"))

    skill_main, skill_secondary = extract_skill(row)

    fields = {
        "PlayerID": pid,
        "Name": normalize(row.get("Nom")),
        "AgeYears": int(row.get("Âge", 0)),
        "AgeDays": int(row.get("Jours", 0)),
        "Specialty": normalize(row.get("Spécialité")),
        "Salary": int(normalize(row.get("Salaire")) or 0),
        "Form": int(normalize(row.get("Forme")) or 0),
        "Stamina": int(normalize(row.get("Endurance")) or 0),
        "SkillMain": skill_main,
        "SkillSecondary": skill_secondary,
        "Position": normalize(row.get("Poste au dernier match")),
        "LastSkillUp": normalize(row.get("Date du dernier match")),
    }

    return fields


# -------------------------------------
# UPSERT
# -------------------------------------

def upsert(csv_players, airtable_index):
    csv_ids = set()

    for row in csv_players:
        player_id = normalize(row.get("ID du joueur"))
        csv_ids.add(player_id)

        fields = build_fields(row)

        # Update
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

        # Create
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


# -------------------------------------
# DELETE MISSING
# -------------------------------------

def delete_missing(airtable_index, csv_ids):
    missing = [pid for pid in airtable_index if pid not in csv_ids]

    if not missing:
        print("🧼 No deletions required.")
        return

    print(f"🧼 Deleting {len(missing)} players...")

    for pid in missing:
        rec_id = airtable_index[pid]
        resp = requests.delete(f"{AIRTABLE_URL}/{rec_id}", headers=airtable_headers)

        if resp.status_code != 200:
            fail(f"Delete error for PlayerID {pid}: {resp.text}")

        print(f"🗑️ Deleted PlayerID {pid}")


# -------------------------------------
# MAIN
# -------------------------------------

def main():
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        fail("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")

    print("🔍 DEBUG Airtable config:")
    print("BASE_ID =", AIRTABLE_BASE_ID)
    print("TABLE =", AIRTABLE_TABLE)
    print("URL =", AIRTABLE_URL)
    print("------")

    csv_players = load_csv_players()
    airtable_index = load_airtable_players()
    csv_ids = upsert(csv_players, airtable_index)

    # ⚠️ Activer après le premier import OK :
    # delete_missing(airtable_index, csv_ids)

    print("✅ Sync complete.")


if __name__ == "__main__":
    main()

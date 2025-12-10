import os
import csv
import sys
import requests

# -------------------------------------
# CONFIG
# -------------------------------------

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# ⚠️ Change ici selon le nom exact de ta table Airtable :
AIRTABLE_TABLE = "Imported table"     # ou "Players" si tu renommes la table

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

airtable_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

# Entraînement principal
TRAINING_TYPE = "Buteur"  # utilisé pour déterminer SkillMain et SkillSecondary


# -------------------------------------
# UTILITIES
# -------------------------------------

def fail(msg):
    print("❌", msg)
    sys.exit(1)


def normalize(s):
    """Normalise totalement les chaînes provenant du CSV :
    - retire BOM
    - remplace espace insécable
    - strip
    """
    if s is None:
        return ""
    return s.replace("\ufeff", "").replace("\u00A0", " ").strip()


# -------------------------------------
# CSV LOADING WITH FULL NORMALIZATION
# -------------------------------------

def load_csv_players(csv_path="players.csv"):
    print(f"⏬ Loading CSV file: {csv_path}")

    if not os.path.exists(csv_path):
        fail(f"CSV file not found: {csv_path}")

    players = []

    with open(csv_path, "r", encoding="utf-8") as f:

        # Détection du séparateur
        sample = f.read(2048)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)

        raw_reader = csv.DictReader(f, dialect=dialect)

        # Normaliser TOUS les noms de colonnes
        fieldmap = {raw: normalize(raw) for raw in raw_reader.fieldnames}

        reader = []
        for raw_row in raw_reader:
            # normaliser les clés + valeurs
            clean = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}
            reader.append(clean)

        # Filtrer les joueurs valides (ID du joueur)
        for row in reader:
            if row.get("ID du joueur"):
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
# SKILL EXTRACTION (training = buteur)
# -------------------------------------

def extract_skill(row):
    """SkillMain = Buteur, SkillSecondary = Passe"""
    def to_int(x):
        try:
            return int(x)
        except:
            return 0

    main = to_int(row.get("Buteur"))
    secondary = to_int(row.get("Passe"))
    return main, secondary


# -------------------------------------
# BUILD RECORD
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
        "Salary": int(row.get("Salaire", row.get("Salaire ", 0))),
        "Form": int(row.get("Forme", 0)),
        "Stamina": int(row.get("Endurance", 0)),
        "SkillMain": skill_main,
        "SkillSecondary": skill_secondary,
        "Position": normalize(row.get("Poste au dernier match")),
        "LastSkillUp": normalize(row.get("Date du dernier match")),
    }

    return fields


# -------------------------------------
# UPSERT (update or create)
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
# DELETE MISSING PLAYERS IN AIRTABLE
# -------------------------------------

def delete_missing(airtable_index, csv_ids):
    missing = [pid for pid in airtable_index if pid not in csv_ids]

    if not missing:
        print("🧼 No deletions required.")
        return

    print(f"🧼 Deleting {len(missing)} players...")

    for pid in missing:
        rec_id = airtable_index[pid]
        resp = requests.delete(f"{AIRTRACK_URL}/{rec_id}", headers=airtable_headers)

        if resp.status_code != 200:
            fail(f"Delete error for PlayerID {pid}: {resp.text}")

        print(f"🗑️ Deleted PlayerID {pid}")


# -------------------------------------
# MAIN
# -------------------------------------

def main():

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        fail("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")

    print("🔍 DEBUG INFO:")
    print("AIRTABLE_BASE_ID =", AIRTABLE_BASE_ID)
    print("Table name =", AIRTABLE_TABLE)
    print("URL =", AIRTABLE_URL)
    print("------")

    csv_players = load_csv_players()
    airtable_index = load_airtable_players()

    csv_ids = upsert(csv_players, airtable_index)

    # delete_missing commented for safety until first real sync
    # delete_missing(airtable_index, csv_ids)

    print("✅ Sync complete.")


if __name__ == "__main__":
    main()

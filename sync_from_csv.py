import os
import csv
import requests

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = "Players"

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

airtable_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}


def debug_config():
    print("=== DEBUG AIRTABLE CONFIG ===")
    print(f"AIRTABLE_BASE_ID: {repr(AIRTABLE_BASE_ID)}")
    if AIRTABLE_API_KEY:
        print(f"AIRTABLE_API_KEY is set, length = {len(AIRTABLE_API_KEY)} chars")
    else:
        print("AIRTABLE_API_KEY is NOT set (None or empty)")

    print(f"AIRTABLE_URL used: {AIRTABLE_URL}")
    print("=== END DEBUG AIRTABLE CONFIG ===")


def test_airtable_connectivity():
    """
    Petit appel direct sur Airtable pour voir la vraie erreur
    avant de lancer toute la sync.
    """
    print("⏬ Testing Airtable connectivity...")
    resp = requests.get(AIRTABLE_URL, headers=airtable_headers)
    print(f"Airtable test status: {resp.status_code}")
    try:
        print("Airtable test response JSON:", resp.json())
    except Exception:
        print("Airtable test raw response:", resp.text)

    if resp.status_code != 200:
        print("❌ Airtable connectivity test FAILED, aborting sync.")
        return False

    print("✅ Airtable connectivity test OK.")
    return True


def load_csv_players(csv_path="players.csv"):
    print(f"⏬ Loading CSV file: {csv_path}")
    players = []

    with open(csv_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.DictReader(f, dialect=dialect)

        for row in reader:
            if not row.get("ID du joueur"):
                continue
            players.append(row)

    print(f"✅ {len(players)} players found in CSV")
    return players


def load_airtable_players():
    print("⏬ Loading existing players from Airtable...")
    index = {}
    params = {"pageSize": 100}

    while True:
        resp = requests.get(AIRTABLE_URL, headers=airtable_headers, params=params)
        if resp.status_code != 200:
            print("❌ Airtable read error:", resp.text)
            break

        data = resp.json()
        for rec in data.get("records", []):
            pid = rec["fields"].get("PlayerID")
            if pid:
                index[str(pid)] = rec["id"]

        if "offset" not in data:
            break
        params["offset"] = data["offset"]

    print(f"✅ {len(index)} players in Airtable")
    return index


def extract_skill(row):
    """Spécialisé pour entraînement BUTEUR"""
    try:
        main = int(row.get("Buteur", 0))
    except Exception:
        main = 0

    try:
        secondary = int(row.get("Passe", 0))
    except Exception:
        secondary = 0

    return main, secondary


def build_fields(row):
    pid = str(row.get("ID du joueur")).strip()

    # skills
    skill_main, skill_secondary = extract_skill(row)

    fields = {
        "PlayerID": pid,
        "Name": row.get("Nom", "").strip(),
        "AgeYears": int(row.get("Âge", 0)),
        "AgeDays": int(row.get("Jours", 0)),
        "Specialty": row.get("Spécialité", "").strip(),
        "Salary": int(row.get("Salaire ", 0)),
        "Form": int(row.get("Forme", 0)),
        "Stamina": int(row.get("Endurance", 0)),
        "SkillMain": skill_main,
        "SkillSecondary": skill_secondary,
        "Position": row.get("Poste au dernier match", "").strip(),
        "LastSkillUp": row.get("Date du dernier match", "").strip()
    }

    return fields


def upsert(csv_players, airtable_index):
    csv_ids = set()

    for row in csv_players:
        player_id = str(row.get("ID du joueur")).strip()
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
                print(f"❌ Update error for {fields['Name']}: {resp.text}")
            else:
                print(f"🔁 Updated {fields['Name']}")
        else:
            resp = requests.post(
                AIRTABLE_URL,
                headers=airtable_headers,
                json={"fields": fields},
            )
            if resp.status_code not in (200, 201):
                print(f"❌ Create error for {fields['Name']}: {resp.text}")
            else:
                print(f"✨ Created {fields['Name']}")

    return csv_ids


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
            print(f"❌ Delete error for PlayerID {pid}: {resp.text}")
        else:
            print(f"🗑️ Deleted PlayerID {pid}")


def main():
    debug_config()

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("❌ AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set. Aborting.")
        return

    # Test simple de connectivité avant d'aller plus loin
    if not test_airtable_connectivity():
        return

    csv_players = load_csv_players()
    airtable_index = load_airtable_players()
    csv_ids = upsert(csv_players, airtable_index)
    delete_missing(airtable_index, csv_ids)
    print("✅ Sync done.")


if __name__ == "__main__":
    main()

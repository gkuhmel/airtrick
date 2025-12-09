import os
import requests
import xml.etree.ElementTree as ET
from requests_oauthlib import OAuth1

# =========================
# SECRETS (GITHUB ACTION)
# =========================
HATTRICK_CONSUMER_KEY = os.getenv("HATTRICK_CONSUMER_KEY")
HATTRICK_CONSUMER_SECRET = os.getenv("HATTRICK_CONSUMER_SECRET")
HATTRICK_ACCESS_TOKEN = os.getenv("HATTRICK_ACCESS_TOKEN")
HATTRICK_ACCESS_SECRET = os.getenv("HATTRICK_ACCESS_SECRET")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = "Players"

# =========================
# AUTH OAUTH HATTRICK
# =========================
auth = OAuth1(
    HATTRICK_CONSUMER_KEY,
    HATTRICK_CONSUMER_SECRET,
    HATTRICK_ACCESS_TOKEN,
    HATTRICK_ACCESS_SECRET
)

# =========================
# HEADERS AIRTABLE
# =========================
airtable_headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# =========================
# 1️⃣ LECTURE DES JOUEURS HATTRICK
# =========================
print("⏬ Récupération joueurs Hattrick...")

url = "https://chpp.hattrick.org/chppxml.ashx?file=players"
response = requests.get(url, auth=auth)

if response.status_code != 200:
    print("❌ Erreur Hattrick :", response.text)
    exit(1)

root = ET.fromstring(response.text)
players = root.findall(".//Player")

print(f"✅ {len(players)} joueurs trouvés")

# =========================
# 2️⃣ LECTURE DES JOUEURS AIRTABLE EXISTANTS (ANTI-DOUBLON)
# =========================
print("⏬ Lecture des joueurs existants dans Airtable...")

airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
existing = requests.get(airtable_url, headers=airtable_headers).json()

airtable_index = {}
for rec in existing.get("records", []):
    pid = rec["fields"].get("PlayerID")
    if pid:
        airtable_index[str(pid)] = rec["id"]

# =========================
# 3️⃣ UPSERT (CREATE / UPDATE)
# =========================
for p in players:
    player_id = p.findtext("PlayerID")

    record = {
        "fields": {
            "PlayerID": player_id,
            "Name": p.findtext("PlayerName"),
            "AgeYears": int(p.findtext("Age")),
            "AgeDays": int(p.findtext("AgeDays")),
            "Specialty": p.findtext("Speciality"),
            "Salary": int(p.findtext("Salary")),
            "Form": int(p.findtext("Form")),
            "Stamina": int(p.findtext("Stamina")),
            "SkillMain": int(p.findtext("Skill")),
            "SkillSecondary": int(p.findtext("Passing")),
            "Position": p.findtext("Position")
        }
    }

    # UPDATE si existe déjà
    if player_id in airtable_index:
        rec_id = airtable_index[player_id]
        r = requests.patch(
            f"{airtable_url}/{rec_id}",
            headers=airtable_headers,
            json=record
        )
        print(f"🔁 Update : {record['fields']['Name']}")

    # CREATE sinon
    else:
        r = requests.post(
            airtable_url,
            headers=airtable_headers,
            json=record
        )
        print(f"✅ Create : {record['fields']['Name']}")

    if r.status_code not in [200, 201]:
        print("❌ Erreur Airtable :", r.text)

print("✅ Synchronisation terminée avec succès")

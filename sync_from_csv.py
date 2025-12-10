import csv

def normalize(s: str) -> str:
    """Nettoie les chaînes :
    - supprime BOM
    - remplace les insécables
    - supprime les espaces autour."""
    if s is None:
        return ""
    return (
        s.replace("\ufeff", "")   # BOM éventuel
         .replace("\u00A0", " ") # espace insécable
         .replace("\u202F", " ") # narrow no-break space
         .strip()
    )


def load_csv_players(csv_path="players.csv"):
    print(f"⏬ Loading CSV file: {csv_path}")

    # Lire tout le fichier brut
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()

    # Normalisation globale
    raw = raw.replace("\u00A0", " ").replace("\u202F", " ")

    # Découpage en lignes
    lines = raw.splitlines()

    # Lecture CSV forcée à la virgule
    reader = csv.DictReader(lines, delimiter=",")

    # Normaliser les noms de colonnes
    raw_fieldnames = reader.fieldnames or []
    normalized_fieldnames = [normalize(col) for col in raw_fieldnames]

    print("🔎 Debug header normalized:", normalized_fieldnames)

    # Mapping : clé brute -> clé propre
    fieldmap = dict(zip(raw_fieldnames, normalized_fieldnames))

    # Détection robuste de la colonne PlayerID
    pid_key = None
    for col in normalized_fieldnames:
        if "id du joueur" in col.lower():
            pid_key = col
            break

    if not pid_key:
        fail("Impossible de détecter la colonne ID du joueur")

    print("🧩 PlayerID column detected:", repr(pid_key))

    players = []

    for raw_row in reader:
        # Normalisation de toutes les valeurs
        row = {fieldmap[k]: normalize(v) for k, v in raw_row.items()}

        pid_value = row.get(pid_key)

        # Joueur valide → ID non vide
        if pid_value and pid_value.isdigit():
            players.append(row)

    print(f"✅ {len(players)} players found in CSV")

    # Debug : premier joueur lu
    if players:
        p = players[0]
        print("👀 First CSV player:", p.get("Nom"), "-", p.get(pid_key))

    return players

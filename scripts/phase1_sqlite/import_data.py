import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data") / "imdb.db"
CSV_DIR = Path("data") / "csv"

def connect_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Connexion à SQLite avec les FK activées."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def normalize_column(col: str):
    """
    Dans tes CSV, les colonnes ont ce format bizarre :
        "('mid',)"   → on doit convertir en : mid
        "('primaryTitle',)" → primaryTitle
    """
    return col.replace("('", "").replace("',)", "")


def load_csv_rows(csv_file: Path):
    """Charge un CSV et nettoie les noms de colonnes + remplace '' par None."""
    with csv_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Normaliser les noms de colonnes
        reader.fieldnames = [normalize_column(c) for c in reader.fieldnames]

        rows = []
        for row in reader:
            clean_row = {}

            for k, v in row.items():
                # Nettoyage des valeurs vides
                if v == "":
                    clean_row[k] = None
                    continue

                # Conversion automatique en int / float si possible
                if v.isdigit():
                    clean_row[k] = int(v)
                else:
                    try:
                        clean_row[k] = float(v)
                    except ValueError:
                        clean_row[k] = v

            rows.append(clean_row)

        return rows


def import_table(conn, table_name, csv_name, columns):
    csv_path = CSV_DIR / csv_name

    if not csv_path.exists():
        print(f"❌ CSV introuvable : {csv_path}")
        return

    print(f"\n📥 Importation de : {table_name}")

    rows = load_csv_rows(csv_path)
    total = len(rows)

    if total == 0:
        print("⚠️ Aucun élément trouvé dans le CSV.")
        return

    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cur = conn.cursor()
    cur.execute("BEGIN")

    inserted = 0
    errors = 0

    for row in rows:
        try:
            values = [row.get(c, None) for c in columns]
            cur.execute(sql, values)
            inserted += 1
        except Exception as e:
            errors += 1
            # print(f"Erreur sur {table_name}: {e}")

    conn.commit()

    print(f"  ✔ Total CSV      : {total}")
    print(f"  ✔ Insérées       : {inserted}")
    print(f"  ❌ Erreurs        : {errors}")


def main():
    if not DB_PATH.exists():
        print("❌ Base imdb.db introuvable ! Exécute create_schema.py d'abord.")
        return

    conn = connect_db()

    try:
        # 1️⃣ Tables indépendantes
        import_table(conn, "movies", "movies.csv",
                     ["mid", "titleType", "primaryTitle", "originalTitle",
                      "isAdult", "startYear", "endYear", "runtimeMinutes"])

        import_table(conn, "persons", "persons.csv",
                     ["pid", "primaryName", "birthYear", "deathYear"])

        # 2️⃣ Dépendantes de persons
        import_table(conn, "professions", "professions.csv",
                     ["pid", "jobName"])

        # 3️⃣ Dépendantes de movies
        import_table(conn, "ratings", "ratings.csv",
                     ["mid", "averageRating", "numVotes"])

        import_table(conn, "genres", "genres.csv",
                     ["mid", "genre"])

        import_table(conn, "titles", "titles.csv",
                     ["mid", "ordering", "title", "region", "language",
                      "types", "attributes", "isOriginalTitle"])

        # 4️⃣ Dépendantes des deux
        import_table(conn, "directors", "directors.csv",
                     ["mid", "pid"])

        import_table(conn, "writers", "writers.csv",
                     ["mid", "pid"])

        import_table(conn, "principals", "principals.csv",
                     ["mid", "ordering", "pid", "category", "job"])

        import_table(conn, "characters", "characters.csv",
                     ["mid", "pid", "name"])

        import_table(conn, "knownformovies", "knownformovies.csv",
                     ["pid", "mid"])

    finally:
        conn.close()
        print("\n🎉 Import terminé avec succès !")


if __name__ == "__main__":
    main()

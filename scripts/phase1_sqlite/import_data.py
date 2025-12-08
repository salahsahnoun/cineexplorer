import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data") / "imdb.db"
CSV_DIR = Path("data") / "csv"


def connect_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Connexion à la base SQLite avec les clés étrangères activées."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def load_csv_rows(csv_file: Path):
    """Charge un CSV et renvoie une liste de dictionnaires (une par ligne)."""
    rows = []
    with csv_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # On peut éventuellement nettoyer ici (remplacer '' par None, etc.)
            clean_row = {k: (v if v != "" else None) for k, v in row.items()}
            rows.append(clean_row)
    return rows


def import_table(conn: sqlite3.Connection, table_name: str, csv_name: str, columns: list[str]):
    """
    Importe une table à partir d'un CSV.
    - conn : connexion SQLite
    - table_name : nom de la table SQL
    - csv_name : nom du fichier CSV (dans data/csv)
    - columns : liste des colonnes dans l'ordre d'insertion
    """
    csv_path = CSV_DIR / csv_name
    if not csv_path.exists():
        print(f"CSV non trouvé pour {table_name}: {csv_path}")
        return

    print(f"\n Import de {table_name} depuis {csv_path} …")

    rows = load_csv_rows(csv_path)
    total = len(rows)
    inserted = 0
    errors = 0

    if total == 0:
        print(f"   (aucune ligne à importer)")
        return

    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    # Transaction pour performance
    cur = conn.cursor()
    cur.execute("BEGIN")
    for row in rows:
        try:
            values = [row[col] for col in columns]
            cur.execute(sql, values)
            inserted += 1
        except sqlite3.IntegrityError as e:
            # Problème de clé étrangère, doublon, etc.
            errors += 1
            # On logge juste le type d’erreur, pas besoin de spammer toutes les lignes
            # (tu peux décommenter si tu veux voir plus de détails)
            # print(f"   [ERREUR] {table_name} : {e} pour la ligne {row}")
        except Exception as e:
            errors += 1
            # print(f"   [ERREUR GENERALE] {table_name} : {e} pour la ligne {row}")

    conn.commit()

    print(f"   ✔ Lignes lues :     {total}")
    print(f"   ✔ Lignes insérées : {inserted}")
    print(f"    Erreurs :        {errors}")


def main():
    if not DB_PATH.exists():
        print(f"❌ Base de données non trouvée : {DB_PATH}")
        print("   Lance d'abord create_schema.py pour créer le schéma.")
        return

    conn = connect_db()

    try:
        # 1. Tables parentes
        import_table(
            conn,
            table_name="movies",
            csv_name="movies.csv",
            columns=[
                "mid",
                "titleType",
                "primaryTitle",
                "originalTitle",
                "isAdult",
                "startYear",
                "endYear",
                "runtimeMinutes",
            ],
        )

        import_table(
            conn,
            table_name="persons",
            csv_name="persons.csv",
            columns=[
                "pid",
                "primaryName",
                "birthYear",
                "deathYear",
            ],
        )

        # 2. Tables dépendantes de persons
        import_table(
            conn,
            table_name="professions",
            csv_name="professions.csv",
            columns=[
                "pid",
                "jobName",
            ],
        )

        # 3. Tables dépendantes de movies uniquement
        import_table(
            conn,
            table_name="ratings",
            csv_name="ratings.csv",
            columns=[
                "mid",
                "averageRating",
                "numVotes",
            ],
        )

        import_table(
            conn,
            table_name="genres",
            csv_name="genres.csv",
            columns=[
                "mid",
                "genre",
            ],
        )

        import_table(
            conn,
            table_name="titles",
            csv_name="titles.csv",
            columns=[
                "mid",
                "ordering",
                "title",
                "region",
                "language",
                "types",
                "attributes",
                "isOriginalTitle",
            ],
        )

        # 4. Tables dépendantes de movies + persons
        import_table(
            conn,
            table_name="directors",
            csv_name="directors.csv",
            columns=[
                "mid",
                "pid",
            ],
        )

        import_table(
            conn,
            table_name="writers",
            csv_name="writers.csv",
            columns=[
                "mid",
                "pid",
            ],
        )

        import_table(
            conn,
            table_name="principals",
            csv_name="principals.csv",
            columns=[
                "mid",
                "ordering",
                "pid",
                "category",
                "job",
            ],
        )

        import_table(
            conn,
            table_name="characters",
            csv_name="characters.csv",
            columns=[
                "mid",
                "pid",
                "name",
            ],
        )

        import_table(
            conn,
            table_name="knownformovies",
            csv_name="knownformovies.csv",
            columns=[
                "pid",
                "mid",
            ],
        )

    finally:
        conn.close()
        print("\n Import terminé.")


if __name__ == "__main__":
    main()

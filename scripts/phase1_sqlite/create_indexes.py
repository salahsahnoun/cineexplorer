import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "imdb.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("➡️ Création des index (Phase T1.4)...")

    cur.executescript(
        """
        ---------------------------------------------------------
        -- INDEXES POUR ACCÉLÉRER T1.3
        ---------------------------------------------------------

        -- Recherche d'acteurs par nom (Q1, Q6)
        CREATE INDEX IF NOT EXISTS idx_persons_primaryName
            ON persons(primaryName);

        -- Accélérer les jointures persons <-> principals (Q1, Q6, Q8)
        CREATE INDEX IF NOT EXISTS idx_principals_pid
            ON principals(pid);

        -- Accélérer les jointures principals <-> movies (Q1, Q6, Q8)
        CREATE INDEX IF NOT EXISTS idx_principals_mid
            ON principals(mid);

        -- Filtrage par genre + jointures (Q2, Q5, Q7)
        CREATE INDEX IF NOT EXISTS idx_genres_genre
            ON genres(genre);

        -- Recherche par période / décennie (Q2, Q6)
        CREATE INDEX IF NOT EXISTS idx_movies_startYear
            ON movies(startYear);

        -- Classements par notes (Q2, Q5, Q7, Q8)
        CREATE INDEX IF NOT EXISTS idx_ratings_avg_numVotes
            ON ratings(averageRating DESC, numVotes DESC);

        -- Jointures pour collaborations acteur-réalisateur (Q4, Q9)
        CREATE INDEX IF NOT EXISTS idx_directors_pid
            ON directors(pid);

        CREATE INDEX IF NOT EXISTS idx_writers_pid
            ON writers(pid);

        -- Rôles multiples d’un acteur dans un film (Q3)
        CREATE INDEX IF NOT EXISTS idx_characters_pid_mid
            ON characters(pid, mid);

        -- Carrières propulsées (Q8)
        CREATE INDEX IF NOT EXISTS idx_known_pid_mid
            ON knownformovies(pid, mid);

        ---------------------------------------------------------
        -- Index supplémentaires utiles mais non obligatoires
        ---------------------------------------------------------

        -- Faciliter les jointures movies.mid (optimisation générale)
        CREATE INDEX IF NOT EXISTS idx_movies_mid
            ON movies(mid);

        -- Faciliter les jointures ratings.mid (optimisation générale)
        CREATE INDEX IF NOT EXISTS idx_ratings_mid
            ON ratings(mid);
        """
    )

    conn.commit()
    conn.close()
    print("✅ Index créés avec succès.")


if __name__ == "__main__":
    main()

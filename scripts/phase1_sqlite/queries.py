import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "imdb.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Ouvre une connexion à la base SQLite imdb.db.

    Args:
        db_path: Chemin vers le fichier SQLite.

    Returns:
        Connexion sqlite3 ouverte.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# 1. Filmographie d’un acteur -------------------------------------------------


def query_actor_filmography(conn: sqlite3.Connection, actor_name: str) -> list[tuple]:
    """
    Retourne la filmographie d’un acteur donné.

    Args:
        conn: Connexion SQLite.
        actor_name: Nom (ou partie du nom) de l’acteur,
                    ex: "Tom Hanks".

    Returns:
        Liste de tuples (titre, année, personnage, note_moyenne).

    SQL utilisé (principe):
        SELECT m.primaryTitle, m.startYear, c.name, r.averageRating
        FROM persons p
        JOIN principals pr ON pr.pid = p.pid
        JOIN movies m ON m.mid = pr.mid
        LEFT JOIN characters c ON c.mid = pr.mid AND c.pid = pr.pid
        LEFT JOIN ratings r ON r.mid = m.mid
        WHERE p.primaryName LIKE '%actor_name%'
          AND pr.category IN ('actor', 'actress');
    """
    sql = """
    SELECT
        m.primaryTitle,
        m.startYear,
        COALESCE(c.name, '') AS characterName,
        r.averageRating
    FROM persons p
    JOIN principals pr ON pr.pid = p.pid
    JOIN movies m ON m.mid = pr.mid
    LEFT JOIN characters c
        ON c.mid = pr.mid AND c.pid = pr.pid
    LEFT JOIN ratings r
        ON r.mid = m.mid
    WHERE p.primaryName LIKE ?
      AND pr.category IN ('actor', 'actress')
    ORDER BY m.startYear, m.primaryTitle;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# 2. Top N films par genre et période -----------------------------------------


def query_top_movies_by_genre_period(
    conn: sqlite3.Connection,
    genre: str,
    start_year: int,
    end_year: int,
    limit: int,
) -> list[tuple]:
    """
    Retourne les N meilleurs films (par note moyenne) pour un genre
    et une période donnée.

    Args:
        conn: Connexion SQLite.
        genre: Nom du genre (ex: 'Drama').
        start_year: Année de début (incluse).
        end_year: Année de fin (incluse).
        limit: Nombre de films à retourner.

    Returns:
        Liste de tuples (titre, année, note_moyenne, nb_votes).

    SQL utilisé (principe):
        SELECT ...
        FROM movies m
        JOIN genres g ON g.mid = m.mid
        JOIN ratings r ON r.mid = m.mid
        WHERE g.genre = genre
          AND m.startYear BETWEEN start_year AND end_year
        ORDER BY r.averageRating DESC
        LIMIT N;
    """
    sql = """
    SELECT
        m.primaryTitle,
        m.startYear,
        r.averageRating,
        r.numVotes
    FROM movies m
    JOIN genres g ON g.mid = m.mid
    JOIN ratings r ON r.mid = m.mid
    WHERE g.genre = ?
      AND m.startYear BETWEEN ? AND ?
    ORDER BY r.averageRating DESC, r.numVotes DESC, m.primaryTitle
    LIMIT ?;
    """
    return conn.execute(sql, (genre, start_year, end_year, limit)).fetchall()


# 3. Acteurs avec plusieurs rôles dans un même film ---------------------------


def query_multi_role_actors(conn: sqlite3.Connection) -> list[tuple]:
    """
    Trouve les acteurs/actrices qui ont joué plusieurs personnages
    dans un même film.

    Returns:
        Liste de tuples (nom_acteur, titre_film, annee, nb_roles).

    SQL utilisé (principe):
        SELECT p.primaryName, m.primaryTitle, COUNT(DISTINCT c.name)
        FROM characters c
        JOIN persons p ON p.pid = c.pid
        JOIN movies m ON m.mid = c.mid
        GROUP BY c.pid, c.mid
        HAVING COUNT(DISTINCT c.name) > 1;
    """
    sql = """
    SELECT
        p.primaryName,
        m.primaryTitle,
        m.startYear,
        COUNT(DISTINCT c.name) AS nb_roles
    FROM characters c
    JOIN persons p ON p.pid = c.pid
    JOIN movies m ON m.mid = c.mid
    GROUP BY c.pid, c.mid
    HAVING COUNT(DISTINCT c.name) > 1
    ORDER BY nb_roles DESC, p.primaryName, m.startYear;
    """
    return conn.execute(sql).fetchall()


# 4. Collaborations acteur ↔ réalisateurs -------------------------------------


def query_actor_director_collaborations(
    conn: sqlite3.Connection,
    actor_name: str,
) -> list[tuple]:
    """
    Pour un acteur donné, retourne les réalisateurs avec lesquels il a
    travaillé et le nombre de films en commun.

    Args:
        conn: Connexion SQLite.
        actor_name: Nom (ou partie du nom) de l’acteur.

    Returns:
        Liste de tuples (pid_realisateur, nom_realisateur, nb_films).

    SQL utilisé (principe avec sous-requête):
        WITH actor_movies AS (
            SELECT DISTINCT pr.mid
            FROM persons p
            JOIN principals pr ON pr.pid = p.pid
            WHERE p.primaryName LIKE '%actor_name%'
              AND pr.category IN ('actor', 'actress')
        )
        SELECT d.pid, dp.primaryName, COUNT(*)
        FROM actor_movies am
        JOIN directors d ON d.mid = am.mid
        JOIN persons dp ON dp.pid = d.pid
        GROUP BY d.pid, dp.primaryName;
    """
    sql = """
    WITH actor_movies AS (
        SELECT DISTINCT pr.mid
        FROM persons p
        JOIN principals pr ON pr.pid = p.pid
        WHERE p.primaryName LIKE ?
          AND pr.category IN ('actor', 'actress')
    )
    SELECT
        d.pid AS director_pid,
        dp.primaryName AS director_name,
        COUNT(*) AS nb_films
    FROM actor_movies am
    JOIN directors d ON d.mid = am.mid
    JOIN persons dp ON dp.pid = d.pid
    GROUP BY d.pid, dp.primaryName
    ORDER BY nb_films DESC, director_name;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# 5. Genres populaires ---------------------------------------------------------


def query_popular_genres(conn: sqlite3.Connection) -> list[tuple]:
    """
    Trouve les genres ayant une note moyenne > 7.0
    et plus de 50 films.

    Returns:
        Liste de tuples (genre, note_moyenne, nb_films).

    SQL utilisé (principe):
        SELECT g.genre, AVG(r.averageRating), COUNT(DISTINCT g.mid)
        FROM genres g
        JOIN ratings r ON r.mid = g.mid
        GROUP BY g.genre
        HAVING AVG(r.averageRating) > 7.0
           AND COUNT(DISTINCT g.mid) > 50;
    """
    sql = """
    SELECT
        g.genre,
        AVG(r.averageRating) AS avg_rating,
        COUNT(DISTINCT g.mid) AS nb_films
    FROM genres g
    JOIN ratings r ON r.mid = g.mid
    GROUP BY g.genre
    HAVING avg_rating > 7.0
       AND nb_films > 50
    ORDER BY avg_rating DESC, nb_films DESC, g.genre;
    """
    return conn.execute(sql).fetchall()


# 6. Évolution de carrière d’un acteur par décennie ---------------------------


def query_actor_career_by_decade(
    conn: sqlite3.Connection,
    actor_name: str,
) -> list[tuple]:
    """
    Pour un acteur donné, calcule le nombre de films par décennie
    et la note moyenne associée.

    Args:
        conn: Connexion SQLite.
        actor_name: Nom (ou partie du nom) de l’acteur.

    Returns:
        Liste de tuples (decennie, nb_films, note_moyenne).

    SQL utilisé (principe avec CTE):
        WITH actor_movies AS (
            SELECT DISTINCT m.mid, m.startYear
            FROM persons p
            JOIN principals pr ON pr.pid = p.pid
            JOIN movies m ON m.mid = pr.mid
            WHERE p.primaryName LIKE '%actor_name%'
              AND pr.category IN ('actor','actress')
        ),
        actor_rated AS (
            SELECT
                am.mid,
                (am.startYear / 10) * 10 AS decade,
                r.averageRating
            FROM actor_movies am
            LEFT JOIN ratings r ON r.mid = am.mid
        )
        SELECT decade, COUNT(*), AVG(averageRating)
        FROM actor_rated
        GROUP BY decade;
    """
    sql = """
    WITH actor_movies AS (
        SELECT DISTINCT m.mid, m.startYear
        FROM persons p
        JOIN principals pr ON pr.pid = p.pid
        JOIN movies m ON m.mid = pr.mid
        WHERE p.primaryName LIKE ?
          AND pr.category IN ('actor','actress')
          AND m.startYear IS NOT NULL
    ),
    actor_rated AS (
        SELECT
            am.mid,
            (am.startYear / 10) * 10 AS decade,
            r.averageRating
        FROM actor_movies am
        LEFT JOIN ratings r ON r.mid = am.mid
    )
    SELECT
        decade,
        COUNT(*) AS nb_films,
        AVG(averageRating) AS avg_rating
    FROM actor_rated
    GROUP BY decade
    ORDER BY decade;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# 7. Top 3 films par genre (classement avec window function) ------------------


def query_top3_by_genre(conn: sqlite3.Connection) -> list[tuple]:
    """
    Pour chaque genre, retourne les 3 meilleurs films (par note moyenne),
    avec un rang calculé via une window function.

    Returns:
        Liste de tuples (genre, titre, note, nb_votes, rang).

    SQL utilisé (principe avec ROW_NUMBER):
        WITH genre_movies AS (...),
        ranked AS (
            SELECT
               genre, primaryTitle, averageRating,
               ROW_NUMBER() OVER (
                   PARTITION BY genre
                   ORDER BY averageRating DESC, numVotes DESC
               ) AS rk
            FROM genre_movies
        )
        SELECT ... WHERE rk <= 3;
    """
    sql = """
    WITH genre_movies AS (
        SELECT
            g.genre,
            m.primaryTitle,
            r.averageRating,
            r.numVotes
        FROM genres g
        JOIN movies m ON m.mid = g.mid
        JOIN ratings r ON r.mid = m.mid
    ),
    ranked AS (
        SELECT
            genre,
            primaryTitle,
            averageRating,
            numVotes,
            ROW_NUMBER() OVER (
                PARTITION BY genre
                ORDER BY averageRating DESC, numVotes DESC, primaryTitle
            ) AS rk
        FROM genre_movies
    )
    SELECT
        genre,
        primaryTitle,
        averageRating,
        numVotes,
        rk
    FROM ranked
    WHERE rk <= 3
    ORDER BY genre, rk;
    """
    return conn.execute(sql).fetchall()


# 8. Carrières "propulsées" par un film ---------------------------------------


def query_boosted_careers(
    conn: sqlite3.Connection,
    min_votes_threshold: int = 200_000,
    limit: int = 50,
) -> list[tuple]:
    """
    Trouve les personnes dont la carrière semble avoir été propulsée
    par un film à fort nombre de votes.

    Critère simplifié:
        - min(numVotes) < seuil
        - max(numVotes) >= seuil
      sur l'ensemble de leur filmographie.

    Args:
        conn: Connexion SQLite.
        min_votes_threshold: Seuil de votes (par défaut 200000).
        limit: Nombre maximum de personnes à retourner.

    Returns:
        Liste de tuples (pid, nom, nb_films, min_votes, max_votes).

    SQL utilisé (principe):
        WITH person_movies AS (...),
        agg AS (
            SELECT pid, primaryName,
                   MIN(numVotes), MAX(numVotes), COUNT(*)
            FROM person_movies
            GROUP BY pid, primaryName
        )
        SELECT ...
        WHERE min_votes < seuil AND max_votes >= seuil;
    """
    sql = """
    WITH person_movies AS (
        SELECT DISTINCT
            p.pid,
            p.primaryName,
            r.numVotes
        FROM persons p
        JOIN principals pr ON pr.pid = p.pid
        JOIN ratings r ON r.mid = pr.mid
    ),
    agg AS (
        SELECT
            pid,
            primaryName,
            MIN(numVotes) AS min_votes,
            MAX(numVotes) AS max_votes,
            COUNT(*) AS nb_films
        FROM person_movies
        GROUP BY pid, primaryName
    )
    SELECT
        pid,
        primaryName,
        nb_films,
        min_votes,
        max_votes
    FROM agg
    WHERE min_votes < ?
      AND max_votes >= ?
    ORDER BY max_votes DESC, nb_films DESC, primaryName
    LIMIT ?;
    """
    return conn.execute(
        sql, (min_votes_threshold, min_votes_threshold, limit)
    ).fetchall()


# 9. Requête libre : top réalisateurs par note moyenne ------------------------


def query_top_directors(
    conn: sqlite3.Connection,
    min_movies: int = 5,
    limit: int = 50,
) -> list[tuple]:
    """
    Requête libre : retourne les réalisateurs ayant au moins `min_movies`
    films notés, classés par note moyenne décroissante.

    Args:
        conn: Connexion SQLite.
        min_movies: Nombre minimum de films réalisés.
        limit: Nombre maximum de réalisateurs retournés.

    Returns:
        Liste de tuples (pid_realisateur, nom, nb_films, note_moyenne).

    SQL utilisé (principe, >= 3 JOIN):
        WITH director_movies AS (
            SELECT d.pid, p.primaryName, m.mid, m.primaryTitle, r.averageRating
            FROM directors d
            JOIN persons p ON p.pid = d.pid
            JOIN movies m ON m.mid = d.mid
            JOIN ratings r ON r.mid = m.mid
        ),
        agg AS (
            SELECT pid, primaryName,
                   COUNT(*) AS nb_films,
                   AVG(averageRating) AS avg_rating
            FROM director_movies
            GROUP BY pid, primaryName
            HAVING COUNT(*) >= min_movies
        )
        SELECT ...
        ORDER BY avg_rating DESC;
    """
    sql = """
    WITH director_movies AS (
        SELECT
            d.pid AS director_pid,
            p.primaryName AS director_name,
            m.mid,
            m.primaryTitle,
            r.averageRating
        FROM directors d
        JOIN persons p ON p.pid = d.pid
        JOIN movies m ON m.mid = d.mid
        JOIN ratings r ON r.mid = m.mid
    ),
    agg AS (
        SELECT
            director_pid,
            director_name,
            COUNT(*) AS nb_films,
            AVG(averageRating) AS avg_rating
        FROM director_movies
        GROUP BY director_pid, director_name
        HAVING nb_films >= ?
    )
    SELECT
        director_pid,
        director_name,
        nb_films,
        avg_rating
    FROM agg
    ORDER BY avg_rating DESC, nb_films DESC, director_name
    LIMIT ?;
    """
    return conn.execute(sql, (min_movies, limit)).fetchall()


if __name__ == "__main__":
    # Petit test manuel (optionnel)
    conn = get_connection()
    try:
        print("Exemple filmographie (Tom Hanks) :")
        rows = query_actor_filmography(conn, "Tom Hanks")
        for r in rows[:5]:
            print(r)
    finally:
        conn.close()

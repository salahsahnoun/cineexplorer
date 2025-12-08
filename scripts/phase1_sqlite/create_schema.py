import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "imdb.db"

def create_schema(db_path: Path = DB_PATH):
    # Création du répertoire data/ si nécessaire
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Activation des clés étrangères dans SQLite
    cur.execute("PRAGMA foreign_keys = ON;")

    cur.executescript("""
    DROP TABLE IF EXISTS knownformovies;
    DROP TABLE IF EXISTS professions;
    DROP TABLE IF EXISTS characters;
    DROP TABLE IF EXISTS titles;
    DROP TABLE IF EXISTS principals;
    DROP TABLE IF EXISTS writers;
    DROP TABLE IF EXISTS directors;
    DROP TABLE IF EXISTS ratings;
    DROP TABLE IF EXISTS genres;
    DROP TABLE IF EXISTS persons;
    DROP TABLE IF EXISTS movies;

    CREATE TABLE movies (
        mid TEXT PRIMARY KEY,
        titleType TEXT,
        primaryTitle TEXT,
        originalTitle TEXT,
        isAdult INTEGER,
        startYear INTEGER,
        endYear INTEGER,
        runtimeMinutes INTEGER
    );

    CREATE TABLE persons (
        pid TEXT PRIMARY KEY,
        primaryName TEXT,
        birthYear INTEGER,
        deathYear INTEGER
    );

    CREATE TABLE genres (
        mid TEXT NOT NULL,
        genre TEXT NOT NULL,
        PRIMARY KEY (mid, genre),
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );

    CREATE TABLE ratings (
        mid TEXT PRIMARY KEY,
        averageRating REAL,
        numVotes INTEGER,
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );

    CREATE TABLE directors (
        mid TEXT NOT NULL,
        pid TEXT NOT NULL,
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );

    CREATE TABLE writers (
        mid TEXT NOT NULL,
        pid TEXT NOT NULL,
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );

    CREATE TABLE principals (
        mid TEXT NOT NULL,
        ordering INTEGER NOT NULL,
        pid TEXT NOT NULL,
        category TEXT,
        job TEXT,
        PRIMARY KEY (mid, ordering),
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );

    CREATE TABLE titles (
        mid TEXT NOT NULL,
        ordering INTEGER NOT NULL,
        title TEXT,
        region TEXT,
        language TEXT,
        types TEXT,
        attributes TEXT,
        isOriginalTitle INTEGER,
        PRIMARY KEY (mid, ordering),
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );

    CREATE TABLE characters (
        mid TEXT NOT NULL,
        pid TEXT NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (mid, pid, name),
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );

    CREATE TABLE professions (
        pid TEXT NOT NULL,
        jobName TEXT NOT NULL,
        PRIMARY KEY (pid, jobName),
        FOREIGN KEY (pid) REFERENCES persons(pid)
    );

    CREATE TABLE knownformovies (
        pid TEXT NOT NULL,
        mid TEXT NOT NULL,
        PRIMARY KEY (pid, mid),
        FOREIGN KEY (pid) REFERENCES persons(pid),
        FOREIGN KEY (mid) REFERENCES movies(mid)
    );
    """)

    conn.commit()
    conn.close()
    print(f"✅ Schéma relationnel créé dans {db_path}")

if __name__ == "__main__":
    create_schema()


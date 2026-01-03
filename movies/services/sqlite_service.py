"""
Service d'accès à la base SQLite pour T3.3 et Phase 4
"""
import sqlite3
from pathlib import Path
from django.conf import settings
import json

def get_sqlite_connection():
    """Établit une connexion à la base SQLite"""
    db_path = Path(settings.BASE_DIR) / "data" / "imdb.db"
    
    if not db_path.exists():
        raise FileNotFoundError(f"Base SQLite non trouvée : {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row  # Retourne des dictionnaires
    return conn

def get_movie_stats():
    """Récupère des statistiques depuis SQLite"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # 1. Nombre total de films
        cursor.execute("SELECT COUNT(*) FROM movies")
        stats['total_movies'] = cursor.fetchone()[0]
        
        # 2. Nombre total de personnes
        cursor.execute("SELECT COUNT(*) FROM persons")
        stats['total_persons'] = cursor.fetchone()[0]
        
        # 3. Film le mieux noté
        cursor.execute("""
            SELECT m.mid, m.primaryTitle, r.averageRating 
            FROM movies m
            JOIN ratings r ON m.mid = r.mid
            WHERE r.averageRating IS NOT NULL
            ORDER BY r.averageRating DESC
            LIMIT 1
        """)
        best_movie = cursor.fetchone()
        stats['best_movie'] = {
            'id': best_movie[0] if best_movie else None,
            'title': best_movie[1] if best_movie else 'N/A',
            'rating': float(best_movie[2]) if best_movie and best_movie[2] else 0
        }
        
        # 4. Nombre de genres différents
        cursor.execute("SELECT COUNT(DISTINCT genre) FROM genres")
        stats['total_genres'] = cursor.fetchone()[0]
        
        # 5. Année du film le plus récent
        cursor.execute("""
            SELECT MAX(startYear) FROM movies 
            WHERE startYear IS NOT NULL AND startYear != '\\N'
        """)
        latest_year = cursor.fetchone()[0]
        stats['latest_year'] = latest_year if latest_year else 'N/A'
        
        # 6. Année du film le plus ancien
        cursor.execute("""
            SELECT MIN(startYear) FROM movies 
            WHERE startYear IS NOT NULL AND startYear != '\\N'
        """)
        earliest_year = cursor.fetchone()[0]
        stats['earliest_year'] = earliest_year if earliest_year else '1900'
        
        # 7. Note moyenne
        cursor.execute("SELECT AVG(averageRating) FROM ratings")
        avg_rating = cursor.fetchone()[0]
        stats['avg_rating'] = float(avg_rating) if avg_rating else 0
        
        # 8. Note minimale
        cursor.execute("SELECT MIN(averageRating) FROM ratings WHERE averageRating > 0")
        min_rating = cursor.fetchone()[0]
        stats['min_rating'] = float(min_rating) if min_rating else 0
        
        # 9. Note maximale
        cursor.execute("SELECT MAX(averageRating) FROM ratings")
        max_rating = cursor.fetchone()[0]
        stats['max_rating'] = float(max_rating) if max_rating else 10
        
        # 10. Nombre de films par type
        cursor.execute("""
            SELECT titleType, COUNT(*) as count 
            FROM movies 
            GROUP BY titleType 
            ORDER BY count DESC
        """)
        stats['movies_by_type'] = [
            {'type': row[0], 'count': row[1]} 
            for row in cursor.fetchall()
        ]
        
        conn.close()
        return stats
        
    except Exception as e:
        return {'error': str(e)}

def get_extended_stats():
    """Statistiques étendues pour la page stats"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        stats = get_movie_stats()  # Récupérer les stats de base
        
        # Distribution par genre
        cursor.execute("""
            SELECT g.genre, COUNT(*) as count
            FROM genres g
            JOIN movies m ON g.mid = m.mid
            GROUP BY g.genre
            ORDER BY count DESC
            LIMIT 15
        """)
        stats['genres_distribution'] = [
            {'genre': row[0], 'count': row[1]}
            for row in cursor.fetchall()
        ]
        
        # Acteurs les plus prolifiques
        cursor.execute("""
            SELECT p.primaryName, COUNT(*) as movie_count
            FROM principals pr
            JOIN persons p ON pr.pid = p.pid
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY p.pid, p.primaryName
            ORDER BY movie_count DESC
            LIMIT 20
        """)
        stats['top_actors_raw'] = [
            {'name': row[0], 'movie_count': row[1]}
            for row in cursor.fetchall()
        ]
        
        conn.close()
        return stats
        
    except Exception as e:
        return {'error': str(e)}

def get_filtered_movies(genre='', year_from='', year_to='', min_rating='', sort='-rating', limit=None):
    """Récupère des films avec filtres"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        # Construire la requête SQL
        query = """
            SELECT 
                m.mid as id,
                m.primaryTitle as title,
                m.startYear as year,
                m.titleType,
                r.averageRating as rating,
                r.numVotes as votes,
                GROUP_CONCAT(DISTINCT g.genre) as genres_str
            FROM movies m
            LEFT JOIN ratings r ON m.mid = r.mid
            LEFT JOIN genres g ON m.mid = g.mid
            WHERE 1=1
        """
        
        params = []
        
        # Filtre par genre
        if genre:
            query += " AND EXISTS (SELECT 1 FROM genres g2 WHERE g2.mid = m.mid AND g2.genre = ?)"
            params.append(genre)
        
        # Filtre par année
        if year_from and year_from.isdigit():
            query += " AND m.startYear >= ?"
            params.append(int(year_from))
        
        if year_to and year_to.isdigit():
            query += " AND m.startYear <= ?"
            params.append(int(year_to))
        
        # Filtre par note minimale
        if min_rating and min_rating.replace('.', '', 1).isdigit():
            query += " AND r.averageRating >= ?"
            params.append(float(min_rating))
        
        # Grouper par film
        query += " GROUP BY m.mid, m.primaryTitle, m.startYear, m.titleType, r.averageRating, r.numVotes"
        
        # Trier
        sort_mapping = {
            '-rating': 'rating DESC',
            'rating': 'rating ASC',
            '-year': 'year DESC',
            'year': 'year ASC',
            'title': 'title ASC',
            '-title': 'title DESC',
            '-votes': 'votes DESC'
        }
        sort_clause = sort_mapping.get(sort, 'rating DESC')
        query += f" ORDER BY {sort_clause}"
        
        # Limite
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        cursor.execute(query, params)
        
        movies = []
        for row in cursor.fetchall():
            movie = dict(row)
            # Convertir les genres en liste
            if movie['genres_str']:
                movie['genres'] = movie['genres_str'].split(',')
            else:
                movie['genres'] = []
            del movie['genres_str']
            
            # Ajouter des données par défaut si manquantes
            if not movie['rating']:
                movie['rating'] = 0
            if not movie['votes']:
                movie['votes'] = 0
            
            movies.append(movie)
        
        conn.close()
        return movies
        
    except Exception as e:
        print(f"Erreur dans get_filtered_movies: {e}")
        return []

def get_all_genres():
    """Récupère tous les genres distincts"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT genre FROM genres ORDER BY genre")
        genres = [row[0] for row in cursor.fetchall() if row[0]]
        
        conn.close()
        return genres
        
    except Exception as e:
        print(f"Erreur dans get_all_genres: {e}")
        return []

def get_movie_basic_info(movie_id):
    """Récupère les informations de base d'un film depuis SQLite"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.mid,
                m.primaryTitle as title,
                m.startYear as year,
                m.runtimeMinutes as runtime,
                m.titleType,
                r.averageRating as rating,
                r.numVotes as votes
            FROM movies m
            LEFT JOIN ratings r ON m.mid = r.mid
            WHERE m.mid = ?
        """, (movie_id,))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        
        movie = dict(row)
        
        # Récupérer les genres
        cursor.execute("SELECT genre FROM genres WHERE mid = ?", (movie_id,))
        movie['genres'] = [row[0] for row in cursor.fetchall()]
        
        # Récupérer les directeurs
        cursor.execute("""
            SELECT p.primaryName as name
            FROM directors d
            JOIN persons p ON d.pid = p.pid
            WHERE d.mid = ?
        """, (movie_id,))
        movie['directors'] = [{'name': row[0]} for row in cursor.fetchall()]
        
        conn.close()
        return movie
        
    except Exception as e:
        print(f"Erreur dans get_movie_basic_info: {e}")
        return None

def get_similar_movies(movie_id, genres=None, limit=4):
    """Récupère des films similaires (mêmes genres)"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        if not genres:
            # Récupérer les genres du film
            cursor.execute("SELECT genre FROM genres WHERE mid = ?", (movie_id,))
            genres = [row[0] for row in cursor.fetchall()]
        
        if not genres:
            conn.close()
            return []
        
        # Trouver des films avec au moins un genre en commun
        query = """
            SELECT DISTINCT m.mid as id, m.primaryTitle as title, m.startYear as year, r.averageRating as rating
            FROM movies m
            JOIN genres g ON m.mid = g.mid
            LEFT JOIN ratings r ON m.mid = r.mid
            WHERE g.genre IN ({})
              AND m.mid != ?
            ORDER BY r.averageRating DESC
            LIMIT ?
        """.format(','.join(['?'] * len(genres)))
        
        params = genres + [movie_id, limit]
        cursor.execute(query, params)
        
        similar = []
        for row in cursor.fetchall():
            similar.append(dict(row))
        
        conn.close()
        return similar
        
    except Exception as e:
        print(f"Erreur dans get_similar_movies: {e}")
        return []

def get_top_actors(limit=10):
    """Récupère les acteurs les plus prolifiques"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                p.primaryName as name,
                COUNT(DISTINCT pr.mid) as movie_count,
                AVG(r.averageRating) as avg_rating
            FROM principals pr
            JOIN persons p ON pr.pid = p.pid
            LEFT JOIN movies m ON pr.mid = m.mid
            LEFT JOIN ratings r ON m.mid = r.mid
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY p.pid, p.primaryName
            HAVING movie_count >= 5
            ORDER BY movie_count DESC
            LIMIT ?
        """, (limit,))
        
        actors = []
        for row in cursor.fetchall():
            actors.append({
                'name': row[0],
                'movie_count': row[1],
                'avg_rating': float(row[2]) if row[2] else None
            })
        
        conn.close()
        return actors
        
    except Exception as e:
        print(f"Erreur dans get_top_actors: {e}")
        return []

def search_persons(query, limit=20):
    """Recherche de personnes"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                pid as id,
                primaryName as name,
                birthYear,
                deathYear,
                primaryProfession as category
            FROM persons
            WHERE primaryName LIKE ?
               OR primaryProfession LIKE ?
            LIMIT ?
        """, (f'%{query}%', f'%{query}%', limit))
        
        persons = []
        for row in cursor.fetchall():
            person = dict(row)
            
            # Compter le nombre de films
            cursor.execute("""
                SELECT COUNT(*) FROM principals WHERE pid = ?
            """, (person['id'],))
            person['movie_count'] = cursor.fetchone()[0]
            
            persons.append(person)
        
        conn.close()
        return persons
        
    except Exception as e:
        print(f"Erreur dans search_persons: {e}")
        return []

def test_sqlite_connection():
    """Test simple de connexion SQLite"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        return True
    except Exception as e:
        return False
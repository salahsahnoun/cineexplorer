"""
Service d'accès à la base SQLite pour T3.3
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
            SELECT m.primaryTitle, r.averageRating 
            FROM movies m
            JOIN ratings r ON m.mid = r.mid
            WHERE r.averageRating IS NOT NULL
            ORDER BY r.averageRating DESC
            LIMIT 1
        """)
        best_movie = cursor.fetchone()
        stats['best_movie'] = {
            'title': best_movie[0] if best_movie else 'N/A',
            'rating': float(best_movie[1]) if best_movie and best_movie[1] else 0
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
        
        # 6. Nombre de films par type
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
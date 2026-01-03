"""
Services pour la page d'accueil et recherche
"""
from . import sqlite_service, mongo_service
import random

def get_home_stats():
    """Statistiques pour la page d'accueil"""
    stats = {}
    
    # Stats SQLite
    sql_stats = sqlite_service.get_movie_stats()
    stats.update(sql_stats)
    
    # Top 10 films
    stats['top_movies'] = get_top_rated_movies(limit=10)
    
    # Films aléatoires
    stats['random_movies'] = get_random_movies(limit=6)
    
    # Stats MongoDB si disponible
    try:
        mongo_stats = mongo_service.get_mongo_stats()
        if mongo_stats.get('replica_status') == 'ok':
            stats['mongo_available'] = True
            stats['mongo_movies'] = mongo_stats.get('total_movies', 0)
        else:
            stats['mongo_available'] = False
            stats['mongo_movies'] = 0
    except:
        stats['mongo_available'] = False
        stats['mongo_movies'] = 0
    
    return stats

def get_top_rated_movies(limit=10):
    """Top N films les mieux notés"""
    conn = sqlite_service.get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            m.mid as id,
            m.primaryTitle as title,
            m.startYear as year,
            r.averageRating as rating,
            r.numVotes as votes
        FROM movies m
        JOIN ratings r ON m.mid = r.mid
        WHERE m.titleType = 'movie'
          AND r.numVotes > 1000
          AND m.startYear IS NOT NULL
          AND m.startYear != '\\N'
        ORDER BY r.averageRating DESC, r.numVotes DESC
        LIMIT ?
    """, (limit,))
    
    movies = []
    for row in cursor.fetchall():
        movies.append({
            'id': row['id'],
            'title': row['title'],
            'year': row['year'],
            'rating': row['rating'],
            'votes': row['votes']
        })
    
    conn.close()
    return movies

def get_random_movies(limit=6):
    """Films aléatoires pour l'accueil"""
    conn = sqlite_service.get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            m.mid as id,
            m.primaryTitle as title,
            m.startYear as year,
            r.averageRating as rating
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
        WHERE m.titleType = 'movie'
          AND m.startYear IS NOT NULL
          AND m.startYear != '\\N'
          AND m.startYear > 2000
        ORDER BY RANDOM()
        LIMIT ?
    """, (limit,))
    
    movies = []
    for row in cursor.fetchall():
        movies.append({
            'id': row['id'],
            'title': row['title'],
            'year': row['year'],
            'rating': row['rating'] or 0
        })
    
    conn.close()
    return movies

def search_movies(query, limit=20):
    """Recherche de films par titre"""
    conn = sqlite_service.get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            m.mid as id,
            m.primaryTitle as title,
            m.startYear as year,
            r.averageRating as rating,
            r.numVotes as votes,
            m.titleType,
            GROUP_CONCAT(DISTINCT g.genre) as genres_str
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
        LEFT JOIN genres g ON m.mid = g.mid
        WHERE m.primaryTitle LIKE ?
           OR m.originalTitle LIKE ?
        GROUP BY m.mid, m.primaryTitle, m.startYear, r.averageRating, r.numVotes, m.titleType
        ORDER BY r.averageRating DESC NULLS LAST, r.numVotes DESC NULLS LAST
        LIMIT ?
    """, (f'%{query}%', f'%{query}%', limit))
    
    results = []
    for row in cursor.fetchall():
        movie = dict(row)
        
        # Convertir les genres en liste
        if movie.get('genres_str'):
            movie['genres'] = movie['genres_str'].split(',')
        else:
            movie['genres'] = []
        
        if 'genres_str' in movie:
            del movie['genres_str']
        
        # Ajouter le type
        movie['type'] = 'movie'
        
        results.append(movie)
    
    conn.close()
    return results

def search_persons(query, limit=20):
    """Recherche de personnes (acteurs, réalisateurs)"""
    conn = sqlite_service.get_sqlite_connection()
    cursor = conn.cursor()
    
    # D'abord, vérifions les colonnes disponibles
    cursor.execute("PRAGMA table_info(persons);")
    columns = [col[1] for col in cursor.fetchall()]
    print(f"Colonnes disponibles dans persons: {columns}")
    
    # Construire la requête selon les colonnes disponibles
    if 'primaryProfession' in columns:
        select_columns = """
            pid as id,
            primaryName as name,
            birthYear,
            deathYear,
            primaryProfession
        """
        where_clause = "primaryName LIKE ? OR primaryProfession LIKE ?"
        params = (f'%{query}%', f'%{query}%', limit)
    else:
        # Fallback si primaryProfession n'existe pas
        select_columns = """
            pid as id,
            primaryName as name,
            birthYear,
            deathYear
        """
        where_clause = "primaryName LIKE ?"
        params = (f'%{query}%', limit)
    
    sql = f"""
        SELECT 
            {select_columns}
        FROM persons
        WHERE {where_clause}
        LIMIT ?
    """
    
    cursor.execute(sql, params)
    
    persons = []
    for row in cursor.fetchall():
        person = dict(row)
        
        # Gérer la profession selon les colonnes disponibles
        if 'primaryProfession' in person:
            professions = person.get('primaryProfession', '').split(',')
            if professions and professions[0]:
                person['category'] = professions[0]
            else:
                person['category'] = 'Acteur'
        else:
            # Déterminer la catégorie en fonction des autres tables
            person['category'] = 'Personne'
        
        # Compter le nombre total de films
        cursor.execute("""
            SELECT COUNT(DISTINCT mid) 
            FROM principals 
            WHERE pid = ?
        """, (person['id'],))
        person['movie_count'] = cursor.fetchone()[0]
        
        # Compter en tant qu'acteur
        cursor.execute("""
            SELECT COUNT(DISTINCT mid) 
            FROM principals 
            WHERE pid = ? AND category IN ('actor', 'actress')
        """, (person['id'],))
        person['actor_count'] = cursor.fetchone()[0]
        
        # Compter en tant que réalisateur
        cursor.execute("""
            SELECT COUNT(DISTINCT mid) 
            FROM directors 
            WHERE pid = ?
        """, (person['id'],))
        person['director_count'] = cursor.fetchone()[0]
        
        # Compter en tant que scénariste
        cursor.execute("""
            SELECT COUNT(DISTINCT mid) 
            FROM writers 
            WHERE pid = ?
        """, (person['id'],))
        person['writer_count'] = cursor.fetchone()[0]
        
        # Déterminer le rôle principal
        if person['actor_count'] > 0:
            person['main_role'] = 'Acteur'
        elif person['director_count'] > 0:
            person['main_role'] = 'Réalisateur'
        elif person['writer_count'] > 0:
            person['main_role'] = 'Scénariste'
        else:
            person['main_role'] = person.get('category', 'Personne')
        
        # Ajouter le type
        person['type'] = 'person'
        
        persons.append(person)
    
    conn.close()
    return persons

def search_all(query, limit_per_type=10):
    """Recherche combinée films et personnes"""
    movies = search_movies(query, limit=limit_per_type)
    persons = search_persons(query, limit=limit_per_type)
    
    # Combiner et trier par pertinence
    all_results = []
    
    # Films d'abord
    for movie in movies:
        all_results.append(movie)
    
    # Personnes ensuite
    for person in persons:
        all_results.append(person)
    
    return {
        'movies': movies,
        'persons': persons,
        'all': all_results,
        'total_movies': len(movies),
        'total_persons': len(persons),
        'total': len(movies) + len(persons)
    }
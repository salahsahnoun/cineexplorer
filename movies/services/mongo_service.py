"""
Service d'accès à MongoDB Replica Set - Version corrigée pour le casting
"""
from pymongo import MongoClient
from django.conf import settings

def get_mongo_client():
    """Retourne un client MongoDB connecté"""
    try:
        client = MongoClient(
            'localhost:27017',
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=3000
        )
        return client
    except Exception as e:
        print(f"Erreur connexion MongoDB: {e}")
        return None

def get_complete_movie_with_characters(movie_id):
    """Version améliorée qui cherche les personnages dans plusieurs endroits"""
    try:
        client = get_mongo_client()
        if not client:
            return None
            
        db = client['imdb_replica']
        
        print(f"\n=== RECHERCHE FILM {movie_id} ===")
        
        # 1. Chercher le film
        movie_doc = db.movies.find_one({"mid": movie_id})
        if not movie_doc:
            client.close()
            return None
        
        # 2. Construire le film
        movie = {
            'id': movie_id,
            'title': movie_doc.get('primaryTitle', 'Titre inconnu'),
            'year': movie_doc.get('startYear'),
            'runtime': movie_doc.get('runtimeMinutes'),
            'titleType': movie_doc.get('titleType', 'movie'),
            'language': movie_doc.get('language', 'en'),
            'isAdult': movie_doc.get('isAdult', False),
            'genres': [],
            'rating': None,
            'votes': None,
            'cast': [],
            'directors': [],
            'writers': [],
            'titles': []
        }
        
        # 3. Genres
        if 'genres' in db.list_collection_names():
            genres = db.genres.find({"mid": movie_id})
            movie['genres'] = [g.get('genre') for g in genres if g.get('genre')]
        
        # 4. Note
        if 'ratings' in db.list_collection_names():
            rating = db.ratings.find_one({"mid": movie_id})
            if rating:
                movie['rating'] = rating.get('averageRating')
                movie['votes'] = rating.get('numVotes')
        
        # 5. Réalisateurs
        if 'directors' in db.list_collection_names():
            directors = db.directors.find({"mid": movie_id})
            for dir_doc in directors:
                person = db.persons.find_one({"pid": dir_doc.get('pid')})
                if person:
                    movie['directors'].append({
                        'id': person.get('pid'),
                        'name': person.get('primaryName', 'Inconnu'),
                        'birthYear': person.get('birthYear')
                    })
        
        # 6. Scénaristes
        if 'writers' in db.list_collection_names():
            writers = db.writers.find({"mid": movie_id})
            for writer_doc in writers:
                person = db.persons.find_one({"pid": writer_doc.get('pid')})
                if person:
                    movie['writers'].append({
                        'id': person.get('pid'),
                        'name': person.get('primaryName', 'Inconnu'),
                        'category': writer_doc.get('category', 'writer')
                    })
        
        # 7. CASTING - VERSION AMÉLIORÉE
        if 'principals' in db.list_collection_names():
            print(f"\nRécupération du casting...")
            principals = db.principals.find({"mid": movie_id}).sort("ordering", 1)
            
            for principal in principals:
                person_id = principal.get('pid')
                if not person_id:
                    continue
                
                person = db.persons.find_one({"pid": person_id})
                if not person:
                    continue
                
                # STRATÉGIE POUR TROUVER LES PERSONNAGES :
                characters = []
                
                # Méthode 1: Chercher dans 'characters' collection
                if 'characters' in db.list_collection_names():
                    char_docs = db.characters.find({"mid": movie_id, "pid": person_id})
                    for char_doc in char_docs:
                        char = char_doc.get('character')
                        if char and char != 'None' and char != '\\N':
                            characters.append(char)
                
                # Méthode 2: Chercher dans le champ 'job' de principals
                if not characters and 'job' in principal:
                    job = principal.get('job')
                    if job and job not in ['actor', 'actress', 'self', 'director', 'writer']:
                        characters.append(job)
                
                # Méthode 3: Pour les acteurs principaux, utiliser des noms génériques
                if not characters and principal.get('category') in ['actor', 'actress']:
                    ordering = principal.get('ordering', 0)
                    if ordering <= 10:  # Top 10 des acteurs principaux
                        # Générer un nom de personnage basé sur le rang
                        role_names = [
                            'Protagoniste', 'Personnage principal', 'Second rôle', 
                            'Rôle important', 'Personnage central', 'Personnage clé'
                        ]
                        if ordering < len(role_names):
                            characters.append(role_names[ordering])
                        else:
                            characters.append(f'Rôle n°{ordering}')
                
                # Créer l'entrée de casting
                cast_member = {
                    'id': person_id,
                    'name': person.get('primaryName', 'Inconnu'),
                    'characters': characters,
                    'ordering': principal.get('ordering', 0),
                    'category': principal.get('category', 'actor'),
                    'birthYear': person.get('birthYear'),
                    'deathYear': person.get('deathYear')
                }
                
                movie['cast'].append(cast_member)
        
        # 8. Titres alternatifs
        if 'titles' in db.list_collection_names():
            titles = db.titles.find({"mid": movie_id})
            for title_doc in titles:
                if title_doc.get('title') != movie['title']:
                    movie['titles'].append({
                        'region': title_doc.get('region', ''),
                        'title': title_doc.get('title'),
                        'language': title_doc.get('language', '')
                    })
        
        print(f"Résumé: {len(movie['cast'])} acteurs trouvés")
        print(f"Personnages totaux: {sum(len(a.get('characters', [])) for a in movie['cast'])}")
        
        client.close()
        return movie
        
    except Exception as e:
        print(f"Erreur dans get_complete_movie_with_characters: {e}")
        import traceback
        traceback.print_exc()
        return None

# Mettre à jour la fonction existante
def get_complete_movie(movie_id):
    """Wrapper pour la fonction corrigée"""
    return get_complete_movie_with_characters(movie_id)

def get_mongo_stats():
    """Récupère des statistiques depuis MongoDB"""
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_SETTINGS['replica_set']['database']]
        
        stats = {'collections': db.list_collection_names()}
        
        # Essayer de vérifier le replica set
        try:
            admin_db = client.admin
            replica_status = admin_db.command('replSetGetStatus')
            stats['replica_status'] = 'ok'
            stats['set_name'] = replica_status.get('set', 'rs0')
        except:
            stats['replica_status'] = 'standalone'
        
        # Compter les documents dans chaque collection
        collections = ['movies', 'persons', 'ratings', 'genres', 'directors', 'principals', 'movies_complete']
        for coll_name in collections:
            if coll_name in db.list_collection_names():
                try:
                    stats[f'total_{coll_name}'] = db[coll_name].estimated_document_count()
                except:
                    stats[f'total_{coll_name}'] = 0
            else:
                stats[f'total_{coll_name}'] = 0
        
        client.close()
        return stats
        
    except Exception as e:
        return {
            'error': str(e), 
            'replica_status': 'error',
            'collections': []
        }



def assemble_movie_data(db, movie_id, base_movie):
    """Assemble les données d'un film depuis les collections MongoDB"""
    movie = {
        'id': movie_id,
        'title': base_movie.get('primaryTitle') or base_movie.get('title'),
        'year': base_movie.get('startYear') or base_movie.get('year'),
        'runtime': base_movie.get('runtimeMinutes'),
        'titleType': base_movie.get('titleType', 'movie'),
        'language': base_movie.get('language', 'en'),
        'isAdult': base_movie.get('isAdult', False),
        'description': base_movie.get('description', '')
    }
    
    # 1. Genres
    movie['genres'] = []
    if 'genres' in db.list_collection_names():
        genres = db.genres.find({"mid": movie_id})
        movie['genres'] = [genre.get('genre') for genre in genres if genre.get('genre')]
    
    # 2. Note
    movie['rating'] = None
    movie['votes'] = None
    if 'ratings' in db.list_collection_names():
        rating = db.ratings.find_one({"mid": movie_id})
        if rating:
            movie['rating'] = rating.get('averageRating')
            movie['votes'] = rating.get('numVotes')
    
    # 3. Réalisateurs
    movie['directors'] = []
    if 'directors' in db.list_collection_names() and 'persons' in db.list_collection_names():
        directors = db.directors.find({"mid": movie_id})
        for dir_doc in directors:
            person = db.persons.find_one({"pid": dir_doc.get('pid')})
            if person:
                movie['directors'].append({
                    'id': person.get('pid'),
                    'name': person.get('primaryName'),
                    'birthYear': person.get('birthYear')
                })
    
    # 4. Scénaristes
    movie['writers'] = []
    if 'writers' in db.list_collection_names() and 'persons' in db.list_collection_names():
        writers = db.writers.find({"mid": movie_id})
        for writer_doc in writers:
            person = db.persons.find_one({"pid": writer_doc.get('pid')})
            if person:
                movie['writers'].append({
                    'id': person.get('pid'),
                    'name': person.get('primaryName'),
                    'category': writer_doc.get('category', 'writer')
                })
    
    # 5. Casting complet avec personnages
    movie['cast'] = []
    if 'principals' in db.list_collection_names() and 'persons' in db.list_collection_names():
        principals = db.principals.find({"mid": movie_id}).sort("ordering", 1)
        for principal in principals:
            person = db.persons.find_one({"pid": principal.get('pid')})
            if person:
                # Récupérer les personnages depuis la table characters si elle existe
                characters = []
                if 'characters' in db.list_collection_names():
                    char_docs = db.characters.find({
                        "mid": movie_id,
                        "pid": principal.get('pid')
                    })
                    characters = [char.get('character') for char in char_docs if char.get('character')]
                elif principal.get('characters'):
                    characters = [principal.get('characters')]
                
                cast_member = {
                    'id': person.get('pid'),
                    'name': person.get('primaryName'),
                    'characters': characters,
                    'ordering': principal.get('ordering', 0),
                    'category': principal.get('category', 'actor'),
                    'birthYear': person.get('birthYear')
                }
                movie['cast'].append(cast_member)
    
    # 6. Titres alternatifs
    movie['titles'] = []
    if 'titles' in db.list_collection_names():
        titles = db.titles.find({"mid": movie_id})
        for title_doc in titles:
            if title_doc.get('title') != movie['title']:
                movie['titles'].append({
                    'region': title_doc.get('region', ''),
                    'title': title_doc.get('title'),
                    'language': title_doc.get('language', '')
                })
    
    return movie

def format_movie_from_complete(movie_doc):
    """Formate un film depuis la collection movies_complete"""
    if not movie_doc:
        return None
    
    # Mapping des champs
    movie = {
        'id': movie_doc.get('mid') or movie_doc.get('_id'),
        'title': movie_doc.get('primaryTitle') or movie_doc.get('title'),
        'year': movie_doc.get('startYear') or movie_doc.get('year'),
        'runtime': movie_doc.get('runtimeMinutes') or movie_doc.get('runtime'),
        'titleType': movie_doc.get('titleType', 'movie'),
        'language': movie_doc.get('language', 'en'),
        'isAdult': movie_doc.get('isAdult', False),
        'description': movie_doc.get('description', ''),
        'genres': movie_doc.get('genres', []),
    }
    
    # Note et votes
    if 'rating' in movie_doc:
        if isinstance(movie_doc['rating'], dict):
            movie['rating'] = movie_doc['rating'].get('averageRating')
            movie['votes'] = movie_doc['rating'].get('numVotes')
        else:
            movie['rating'] = movie_doc.get('rating')
            movie['votes'] = movie_doc.get('votes')
    
    # Casting, réalisateurs, scénaristes
    movie['cast'] = movie_doc.get('cast', [])
    movie['directors'] = movie_doc.get('directors', [])
    movie['writers'] = movie_doc.get('writers', [])
    movie['titles'] = movie_doc.get('titles', [])
    
    return movie

def get_similar_movies_from_mongo(movie_id, current_genres=None, current_directors=None, limit=4):
    """Récupère des films similaires depuis MongoDB"""
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_SETTINGS['replica_set']['database']]
        
        similar_movies = []
        
        # Si on a des genres, chercher des films avec les mêmes genres
        if current_genres and 'genres' in db.list_collection_names():
            # Trouver des films avec au moins un genre en commun
            pipeline = [
                {"$match": {"genre": {"$in": current_genres}, "mid": {"$ne": movie_id}}},
                {"$group": {"_id": "$mid"}},
                {"$limit": limit * 2}  # Prendre plus pour filtrer ensuite
            ]
            
            genre_matches = db.genres.aggregate(pipeline)
            genre_movie_ids = [doc['_id'] for doc in genre_matches]
            
            # Récupérer les infos de ces films
            if genre_movie_ids and 'movies' in db.list_collection_names():
                movies = db.movies.find({
                    "mid": {"$in": genre_movie_ids},
                    "titleType": "movie"
                }).limit(limit)
                
                for movie in movies:
                    similar_movies.append({
                        'id': movie.get('mid'),
                        'title': movie.get('primaryTitle'),
                        'year': movie.get('startYear'),
                        'titleType': movie.get('titleType')
                    })
        
        # Si pas assez de films similaires, en prendre au hasard
        if len(similar_movies) < limit and 'movies' in db.list_collection_names():
            additional = list(db.movies.aggregate([
                {"$match": {"mid": {"$ne": movie_id}, "titleType": "movie"}},
                {"$sample": {"size": limit - len(similar_movies)}}
            ]))
            
            for movie in additional:
                similar_movies.append({
                    'id': movie.get('mid'),
                    'title': movie.get('primaryTitle'),
                    'year': movie.get('startYear'),
                    'titleType': movie.get('titleType')
                })
        
        client.close()
        return similar_movies[:limit]
        
    except Exception as e:
        print(f"Erreur dans get_similar_movies_from_mongo: {e}")
        return []
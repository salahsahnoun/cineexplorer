"""
Service d'accès à MongoDB Replica Set pour T3.3
"""
from pymongo import MongoClient
from django.conf import settings

def get_mongo_client():
    """Retourne un client MongoDB connecté au Replica Set"""
    try:
        # Nouvelle syntaxe PyMongo
        connection_string = f"mongodb://{','.join(settings.MONGODB_SETTINGS['replica_set']['hosts'])}/"
        
        client = MongoClient(
            connection_string,
            replicaSet=settings.MONGODB_SETTINGS['replica_set']['name'],
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=3000
        )
        return client
    except Exception as e:
        raise ConnectionError(f"Erreur connexion MongoDB: {e}")

def get_mongo_stats():
    """Récupère des statistiques depuis MongoDB avec meilleure gestion d'erreurs"""
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_SETTINGS['replica_set']['database']]
        
        stats = {}
        
        # Vérifier l'état du Replica Set
        admin_db = client.admin
        
        try:
            replica_status = admin_db.command('replSetGetStatus')
            stats['replica_status'] = 'ok'
            stats['set_name'] = replica_status.get('set', 'rs0')
            
            # Trouver le Primary de manière robuste
            primary = replica_status.get('primary', '')
            if not primary:
                # Chercher manuellement dans les membres
                for member in replica_status.get('members', []):
                    if member.get('stateStr') == 'PRIMARY':
                        primary = member.get('name', 'unknown')
                        break
            
            stats['primary'] = primary if primary else 'unknown (en élection?)'
            
            # Compter les membres
            members = replica_status.get('members', [])
            stats['total_members'] = len(members)
            stats['healthy_members'] = sum(1 for m in members if m.get('health') == 1)
            
            # État des membres
            state_counts = {}
            for member in members:
                state = member.get('stateStr', 'UNKNOWN')
                state_counts[state] = state_counts.get(state, 0) + 1
            
            stats['state_counts'] = state_counts
            
        except Exception as e:
            stats['replica_status'] = 'partial'
            stats['error'] = str(e)
            stats['primary'] = 'error'
        
        # Statistiques des collections
        collections = ['movies', 'persons', 'ratings', 'genres', 'directors', 'movies_complete']
        for coll_name in collections:
            if coll_name in db.list_collection_names():
                try:
                    stats[f'total_{coll_name}'] = db[coll_name].estimated_document_count()
                except:
                    stats[f'total_{coll_name}'] = 'error'
            else:
                stats[f'total_{coll_name}'] = 0
        
        # Vérifier si les données sont importées
        total_docs = sum(1 for k in stats.keys() if k.startswith('total_') and isinstance(stats[k], int) and stats[k] > 0)
        stats['data_imported'] = total_docs > 0
        
        client.close()
        return stats
        
    except Exception as e:
        return {
            'error': str(e), 
            'replica_status': 'error',
            'primary': 'connection failed',
            'advice': 'Vérifiez que le Replica Set est démarré: mongosh --port 27017 --eval "rs.status()"'
        }

def get_complete_movie(movie_id="tt0111161"):
    """Récupère un film complet depuis movies_complete"""
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_SETTINGS['replica_set']['database']]
        
        # Essayer movies_complete d'abord
        if 'movies_complete' in db.list_collection_names():
            movie = db.movies_complete.find_one({"_id": movie_id})
            if movie:
                client.close()
                return format_movie_data(movie)
        
        # Sinon, essayer la collection movies
        if 'movies' in db.list_collection_names():
            movie = db.movies.find_one({"_id": movie_id})
            if movie:
                # Enrichir avec les données liées
                movie = enrich_movie_data(db, movie)
                client.close()
                return movie
        
        client.close()
        return None
        
    except Exception as e:
        print(f"Erreur dans get_complete_movie: {e}")
        return None

def enrich_movie_data(db, movie):
    """Enrichit un film avec les données liées"""
    movie_id = movie.get('_id')
    
    # Récupérer les genres
    genres = db.genres.find({"mid": movie_id})
    movie['genres'] = [genre.get('genre') for genre in genres]
    
    # Récupérer les notes
    rating = db.ratings.find_one({"mid": movie_id})
    if rating:
        movie['rating'] = rating.get('averageRating')
        movie['votes'] = rating.get('numVotes')
    
    # Récupérer les directeurs
    directors = db.directors.find({"mid": movie_id})
    movie['directors'] = []
    for dir_doc in directors:
        person = db.persons.find_one({"pid": dir_doc.get('pid')})
        if person:
            movie['directors'].append({
                'person_id': person.get('pid'),
                'name': person.get('primaryName')
            })
    
    # Récupérer le casting
    principals = db.principals.find({"mid": movie_id}).sort("ordering", 1)
    movie['cast'] = []
    for principal in principals:
        person = db.persons.find_one({"pid": principal.get('pid')})
        if person:
            # Récupérer les personnages
            characters = db.characters.find({
                "mid": movie_id,
                "pid": principal.get('pid')
            })
            
            movie['cast'].append({
                'person_id': person.get('pid'),
                'name': person.get('primaryName'),
                'characters': [char.get('character') for char in characters],
                'ordering': principal.get('ordering', 0),
                'category': principal.get('category')
            })
    
    return movie

def format_movie_data(movie_doc):
    """Formate les données d'un film depuis MongoDB"""
    if not movie_doc:
        return None
    
    movie = {
        'id': movie_doc.get('_id'),
        'title': movie_doc.get('primaryTitle') or movie_doc.get('title'),
        'year': movie_doc.get('startYear') or movie_doc.get('year'),
        'runtime': movie_doc.get('runtimeMinutes') or movie_doc.get('runtime'),
        'titleType': movie_doc.get('titleType') or 'movie',
        'genres': movie_doc.get('genres', []),
        'language': movie_doc.get('language', 'en'),
        'isAdult': movie_doc.get('isAdult', False),
        'description': movie_doc.get('description', '')
    }
    
    # Note et votes
    if 'rating' in movie_doc:
        if isinstance(movie_doc['rating'], dict):
            movie['rating'] = movie_doc['rating'].get('average')
            movie['votes'] = movie_doc['rating'].get('votes')
        else:
            movie['rating'] = movie_doc.get('rating')
            movie['votes'] = movie_doc.get('votes')
    
    # Casting
    movie['cast'] = movie_doc.get('cast', [])
    movie['directors'] = movie_doc.get('directors', [])
    movie['writers'] = movie_doc.get('writers', [])
    movie['titles'] = movie_doc.get('titles', [])
    
    return movie

def test_mongo_connection():
    """Test simple de connexion MongoDB"""
    try:
        client = get_mongo_client()
        client.admin.command('ping')
        client.close()
        return True
    except:
        return False
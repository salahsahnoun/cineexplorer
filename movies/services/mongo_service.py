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
            serverSelectionTimeoutMS=5000
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
        collections = ['movies', 'persons', 'ratings', 'genres', 'directors']
        for coll_name in collections:
            if coll_name in db.list_collection_names():
                try:
                    stats[f'total_{coll_name}'] = db[coll_name].estimated_document_count()
                except:
                    stats[f'total_{coll_name}'] = 'error'
            else:
                stats[f'total_{coll_name}'] = 0
        
        # Vérifier si les données sont importées
        total_docs = sum(1 for k in stats.keys() if k.startswith('total_') and isinstance(stats[k], int))
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

def test_mongo_connection():
    """Test simple de connexion MongoDB"""
    try:
        client = get_mongo_client()
        client.admin.command('ping')
        client.close()
        return True
    except:
        return False

def get_complete_movie(movie_id="tt0111161"):
    """Récupère un film complet depuis movies_complete (optionnel pour T3.3)"""
    try:
        client = get_mongo_client()
        db = client[settings.MONGODB_SETTINGS['replica_set']['database']]
        
        if 'movies_complete' not in db.list_collection_names():
            return None
        
        movie = db.movies_complete.find_one({"_id": movie_id})
        client.close()
        return movie
    except:
        return None
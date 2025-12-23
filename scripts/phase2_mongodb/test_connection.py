from pymongo import MongoClient

def test_mongo_connection():
    try:
        client = MongoClient('localhost', 27017)
        # Tester la connexion
        client.admin.command('ping')
        print("✅ Connexion MongoDB réussie")
        print(f"Version serveur: {client.server_info()['version']}")
        # Lister les bases de données
        print("Bases de données:", client.list_database_names())
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return None

if __name__ == "__main__":
    test_mongo_connection()
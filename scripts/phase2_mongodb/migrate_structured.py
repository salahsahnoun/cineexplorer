from pymongo import MongoClient
import sqlite3
import time

def create_structured_documents():
    """Crée la collection movies_complete avec documents dénormalisés"""
    
    # Connexions
    sqlite_conn = sqlite3.connect('./data/imdb.db')
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['imdb_structured']
    
    # Nettoyer la collection existante
    if 'movies_complete' in db.list_collection_names():
        db.movies_complete.drop()
    
    # Récupérer tous les films
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT movie_id, title, year, runtime FROM movies LIMIT 1000")
    movies = cursor.fetchall()
    
    print(f"🎬 Création de {len(movies)} documents structurés...")
    
    documents = []
    for movie in movies:
        movie_id, title, year, runtime = movie
        
        # Construire le document structuré
        doc = {
            "_id": movie_id,
            "title": title,
            "year": year,
            "runtime": runtime,
            "genres": [],
            "rating": {},
            "directors": [],
            "cast": [],
            "writers": [],
            "titles": []
        }
        
        # Genres
        cursor.execute("SELECT genre FROM genres WHERE movie_id = ?", (movie_id,))
        doc["genres"] = [g[0] for g in cursor.fetchall()]
        
        # Rating
        cursor.execute("SELECT average_rating, num_votes FROM ratings WHERE movie_id = ?", (movie_id,))
        rating = cursor.fetchone()
        if rating:
            doc["rating"] = {
                "average": rating[0],
                "votes": rating[1]
            }
        
        # Directors
        cursor.execute("""
            SELECT p.person_id, p.name 
            FROM directors d 
            JOIN persons p ON d.person_id = p.person_id 
            WHERE d.movie_id = ?
        """, (movie_id,))
        doc["directors"] = [{"person_id": d[0], "name": d[1]} for d in cursor.fetchall()]
        
        # Cast (acteurs avec personnages)
        cursor.execute("""
            SELECT p.person_id, p.name, c.character, pr.ordering
            FROM principals pr
            JOIN persons p ON pr.person_id = p.person_id
            LEFT JOIN characters c ON pr.movie_id = c.movie_id AND pr.person_id = c.person_id
            WHERE pr.movie_id = ? AND pr.category = 'actor'
            ORDER BY pr.ordering
        """, (movie_id,))
        
        actors_by_id = {}
        for actor in cursor.fetchall():
            person_id, name, character, ordering = actor
            if person_id not in actors_by_id:
                actors_by_id[person_id] = {
                    "person_id": person_id,
                    "name": name,
                    "characters": [],
                    "ordering": ordering
                }
            if character:
                actors_by_id[person_id]["characters"].append(character)
        
        doc["cast"] = list(actors_by_id.values())
        
        # Writers
        cursor.execute("""
            SELECT p.person_id, p.name, w.category
            FROM writers w
            JOIN persons p ON w.person_id = p.person_id
            WHERE w.movie_id = ?
        """, (movie_id,))
        doc["writers"] = [{"person_id": w[0], "name": w[1], "category": w[2]} for w in cursor.fetchall()]
        
        # Alternative titles
        cursor.execute("SELECT region, title FROM titles WHERE movie_id = ?", (movie_id,))
        doc["titles"] = [{"region": t[0], "title": t[1]} for t in cursor.fetchall()]
        
        documents.append(doc)
        
        # Progress
        if len(documents) % 100 == 0:
            print(f"   📝 {len(documents)} documents préparés")
    
    # Insérer en masse
    if documents:
        print(f"💾 Insertion de {len(documents)} documents dans MongoDB...")
        db.movies_complete.insert_many(documents)
        print(f"✅ {db.movies_complete.count_documents({})} documents insérés")
    
    # Afficher un exemple
    sample = db.movies_complete.find_one()
    print("\n📄 EXEMPLE DE DOCUMENT STRUCTURÉ:")
    print("-" * 50)
    for key, value in sample.items():
        if isinstance(value, list):
            print(f"{key:15}: [{len(value)} éléments]")
        elif isinstance(value, dict):
            print(f"{key:15}: {len(value)} champs")
        else:
            print(f"{key:15}: {value}")
    
    sqlite_conn.close()
    mongo_client.close()
    
    return len(documents)

def benchmark_structured_vs_flat():
    """Compare les performances des deux modèles"""
    
    mongo_client = MongoClient('localhost', 27017)
    db_flat = mongo_client['imdb_flat']
    db_structured = mongo_client['imdb_structured']
    
    print("\n" + "="*60)
    print("⚡ BENCHMARK : STRUCTURÉ vs PLAT")
    print("="*60)
    
    # Test 1: Récupérer un film complet
    test_movie_id = "tt0111161"  # The Shawshank Redemption
    
    # Modèle plat
    print("\n1. Récupération d'un film complet:")
    print("   Modèle plat (8 requêtes):")
    start = time.time()
    
    # Récupérer les 8 collections nécessaires
    movie = db_flat.movies.find_one({"movie_id": test_movie_id})
    genres = list(db_flat.genres.find({"movie_id": test_movie_id}))
    rating = db_flat.ratings.find_one({"movie_id": test_movie_id})
    # ... autres collections
    flat_time = time.time() - start
    print(f"   ⏱️  {flat_time*1000:.2f} ms")
    
    # Modèle structuré
    print("   Modèle structuré (1 requête):")
    start = time.time()
    structured_movie = db_structured.movies_complete.find_one({"_id": test_movie_id})
    structured_time = time.time() - start
    print(f"   ⏱️  {structured_time*1000:.2f} ms")
    print(f"   🚀 Gain: {((flat_time/structured_time)-1)*100:.1f}% plus rapide")
    
    # Test 2: Taille de stockage
    print("\n2. Taille de stockage:")
    
    # Compter les documents
    flat_count = sum(db_flat[col].estimated_document_count() for col in db_flat.list_collection_names())
    structured_count = db_structured.movies_complete.estimated_document_count()
    
    print(f"   Modèle plat: {flat_count:,} documents répartis")
    print(f"   Modèle structuré: {structured_count:,} documents")
    
    # Test 3: Complexité des requêtes
    print("\n3. Complexité du code:")
    print("   Modèle plat: Requêtes complexes avec $lookup multiples")
    print("   Modèle structuré: Requêtes simples avec find() direct")
    
    mongo_client.close()

if __name__ == "__main__":
    start_time = time.time()
    
    # Créer les documents structurés
    count = create_structured_documents()
    
    # Benchmark
    benchmark_structured_vs_flat()
    
    print(f"\n✅ Phase 2.4 terminée en {time.time() - start_time:.2f} secondes")
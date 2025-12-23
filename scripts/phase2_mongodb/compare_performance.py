import sqlite3
from pymongo import MongoClient
import time
import pandas as pd
import matplotlib.pyplot as plt

def compare_sqlite_vs_mongodb():
    """Compare les performances SQLite vs MongoDB"""
    
    print("📊 COMPARAISON SQLite vs MongoDB")
    print("="*50)
    
    # Connexions
    sqlite_conn = sqlite3.connect('./data/imdb.db')
    mongo_client = MongoClient('localhost', 27017)
    
    # Requête de test : Top 10 films dramatiques des années 90
    sql_query = """
    SELECT m.title, m.year, r.average_rating, r.num_votes
    FROM movies m
    JOIN genres g ON m.movie_id = g.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE g.genre = 'Drama' 
      AND m.year BETWEEN 1990 AND 1999
      AND r.num_votes > 10000
    ORDER BY r.average_rating DESC
    LIMIT 10
    """
    
    # Test SQLite
    print("\n🔍 SQLite:")
    start = time.time()
    cursor = sqlite_conn.cursor()
    cursor.execute(sql_query)
    sqlite_results = cursor.fetchall()
    sqlite_time = time.time() - start
    print(f"   ⏱️  Temps: {sqlite_time*1000:.2f} ms")
    print(f"   📄 Résultats: {len(sqlite_results)} films")
    
    # Test MongoDB (plat)
    print("\n🔍 MongoDB (collections plates):")
    start = time.time()
    
    mongo_pipeline = [
        {"$match": {"genre": "Drama"}},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        {"$match": {"movie_info.year": {"$gte": 1990, "$lte": 1999}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "rating_info"
        }},
        {"$unwind": "$rating_info"},
        {"$match": {"rating_info.num_votes": {"$gt": 10000}}},
        {"$project": {
            "title": "$movie_info.title",
            "year": "$movie_info.year",
            "rating": "$rating_info.average_rating",
            "votes": "$rating_info.num_votes"
        }},
        {"$sort": {"rating": -1}},
        {"$limit": 10}
    ]
    
    mongo_results = list(mongo_client.imdb_flat.genres.aggregate(mongo_pipeline))
    mongo_flat_time = time.time() - start
    print(f"   ⏱️  Temps: {mongo_flat_time*1000:.2f} ms")
    print(f"   📄 Résultats: {len(mongo_results)} films")
    
    # Test MongoDB (structuré)
    print("\n🔍 MongoDB (documents structurés):")
    start = time.time()
    
    structured_query = {
        "genres": "Drama",
        "year": {"$gte": 1990, "$lte": 1999},
        "rating.votes": {"$gt": 10000}
    }
    
    structured_results = list(mongo_client.imdb_structured.movies_complete.find(
        structured_query,
        {"title": 1, "year": 1, "rating.average": 1, "rating.votes": 1, "_id": 0}
    ).sort("rating.average", -1).limit(10))
    
    mongo_structured_time = time.time() - start
    print(f"   ⏱️  Temps: {mongo_structured_time*1000:.2f} ms")
    print(f"   📄 Résultats: {len(structured_results)} films")
    
    # Créer un tableau comparatif
    data = {
        "Base de données": ["SQLite", "MongoDB (plat)", "MongoDB (structuré)"],
        "Temps (ms)": [
            round(sqlite_time * 1000, 2),
            round(mongo_flat_time * 1000, 2),
            round(mongo_structured_time * 1000, 2)
        ],
        "Résultats": [len(sqlite_results), len(mongo_results), len(structured_results)],
        "Complexité": ["SQL simple", "Pipeline complexe", "Requête simple"]
    }
    
    df = pd.DataFrame(data)
    print("\n" + "="*60)
    print("📈 TABLEAU COMPARATIF")
    print("="*60)
    print(df.to_string(index=False))
    
    # Visualisation
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df["Base de données"], df["Temps (ms)"], color=['#4CAF50', '#2196F3', '#FF9800'])
    plt.title("Comparaison des performances: SQLite vs MongoDB", fontsize=14, fontweight='bold')
    plt.ylabel("Temps d'exécution (ms)", fontsize=12)
    plt.xlabel("Base de données", fontsize=12)
    
    # Ajouter les valeurs sur les barres
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{height:.1f} ms', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('./reports/phase2_performance_comparison.png', dpi=300)
    print("\n📊 Graphique sauvegardé: reports/phase2_performance_comparison.png")
    
    # Conclusions
    print("\n" + "="*60)
    print("💡 CONCLUSIONS")
    print("="*60)
    print("✅ SQLite: Meilleur pour les requêtes relationnelles simples")
    print("✅ MongoDB (plat): Flexible mais requêtes complexes")
    print("✅ MongoDB (structuré): Meilleures performances pour requêtes ciblées")
    print("✅ Gain structuré vs plat: ", end="")
    if mongo_flat_time > 0:
        gain = ((mongo_flat_time / mongo_structured_time) - 1) * 100
        print(f"{gain:.1f}% plus rapide")
    
    sqlite_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    compare_sqlite_vs_mongodb()
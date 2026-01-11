# verify_import.py
import sqlite3
from pymongo import MongoClient

print("=" * 60)
print("VÉRIFICATION COMPLÈTE DES DONNÉES")
print("=" * 60)

# 1. Connexions
print("\n[1] CONNEXIONS AUX BASES...")
try:
    # SQLite
    sqlite_conn = sqlite3.connect('data/imdb.db')
    sqlite_cursor = sqlite_conn.cursor()
    print("✅ SQLite connecté")
    
    # MongoDB
    mongo_client = MongoClient('localhost:27017', serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client['imdb_replica']
    print("✅ MongoDB connecté")
except Exception as e:
    print(f"❌ Erreur connexion: {e}")
    exit(1)

# 2. Collections MongoDB disponibles
print("\n[2] COLLECTIONS MONGODB DISPONIBLES:")
collections = mongo_db.list_collection_names()
print(f"Nombre de collections: {len(collections)}")
for col in sorted(collections):
    count = mongo_db[col].estimated_document_count()
    print(f"  - {col}: {count:,} documents")

# 3. Comparaison SQLite vs MongoDB
print("\n[3] COMPARAISON SQLITE vs MONGODB:")

tables_to_check = [
    ('movies', 'movies'),
    ('persons', 'persons'),
    ('ratings', 'ratings'),
    ('genres', 'genres'),
    ('directors', 'directors'),
    ('writers', 'writers'),
    ('principals', 'principals'),
    ('characters', 'characters'),
    ('titles', 'titles')
]

for sql_table, mongo_col in tables_to_check:
    print(f"\n  → {sql_table}:")
    
    # Comptage SQLite
    try:
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {sql_table}")
        sqlite_count = sqlite_cursor.fetchone()[0]
        print(f"    SQLite: {sqlite_count:,} lignes")
    except:
        print(f"    SQLite: Table non trouvée")
        sqlite_count = 0
    
    # Comptage MongoDB
    if mongo_col in collections:
        mongo_count = mongo_db[mongo_col].estimated_document_count()
        print(f"    MongoDB: {mongo_count:,} documents")
        
        # Calcul du pourcentage
        if sqlite_count > 0 and mongo_count > 0:
            percentage = (mongo_count / sqlite_count) * 100
            status = "✅" if percentage > 95 else "⚠️" if percentage > 80 else "❌"
            print(f"    {status} Importé: {percentage:.1f}%")
        elif sqlite_count == 0:
            print(f"    ℹ️  Table SQLite vide")
        elif mongo_count == 0:
            print(f"    ❌ Collection MongoDB vide")
    else:
        print(f"    ❌ Collection MongoDB non trouvée")

# 4. Vérification de données spécifiques
print("\n[4] VÉRIFICATION DE DONNÉES SPÉCIFIQUES:")

# Exemple de films
test_movies = [
    ('tt0111161', 'The Shawshank Redemption'),
    ('tt0068646', 'The Godfather'),
    ('tt0133093', 'The Matrix'),
    ('tt1375666', 'Inception'),
    ('tt16747572', 'Film test')
]

print("\n  Films testés:")
for movie_id, movie_title in test_movies:
    print(f"\n  → {movie_title} ({movie_id}):")
    
    # Vérification SQLite
    sqlite_cursor.execute("SELECT primaryTitle FROM movies WHERE mid = ?", (movie_id,))
    sqlite_row = sqlite_cursor.fetchone()
    sqlite_found = bool(sqlite_row)
    print(f"    SQLite: {'✅ Trouvé' if sqlite_found else '❌ Non trouvé'}")
    
    # Vérification MongoDB
    mongo_found = False
    if 'movies' in collections:
        mongo_movie = mongo_db.movies.find_one({"mid": movie_id})
        mongo_found = bool(mongo_movie)
    print(f"    MongoDB: {'✅ Trouvé' if mongo_found else '❌ Non trouvé'}")

# 5. Vérification des relations
print("\n[5] VÉRIFICATION DES RELATIONS:")

# Vérifier un film avec ses relations
test_movie_id = 'tt0111161'
print(f"\n  Relations pour {test_movie_id}:")
relations = [
    ('genres', 'Genres'),
    ('ratings', 'Note'),
    ('directors', 'Réalisateurs'),
    ('principals', 'Acteurs')
]

for col, label in relations:
    if col in collections:
        count = mongo_db[col].count_documents({"mid": test_movie_id})
        print(f"    {label}: {count}")
    else:
        print(f"    {label}: Collection non trouvée")

# 6. Statistiques générales
print("\n[6] STATISTIQUES GÉNÉRALES:")

# SQLite
print("\n  SQLite:")
sqlite_cursor.execute("SELECT COUNT(DISTINCT mid) FROM movies")
movies_count = sqlite_cursor.fetchone()[0]
print(f"    Films totaux: {movies_count:,}")

sqlite_cursor.execute("SELECT COUNT(DISTINCT pid) FROM persons")
persons_count = sqlite_cursor.fetchone()[0]
print(f"    Personnes totales: {persons_count:,}")

# MongoDB
print("\n  MongoDB:")
if 'movies' in collections:
    mongo_movies = mongo_db.movies.estimated_document_count()
    print(f"    Films: {mongo_movies:,}")

if 'persons' in collections:
    mongo_persons = mongo_db.persons.estimated_document_count()
    print(f"    Personnes: {mongo_persons:,}")

# 7. Vérification de l'intégrité
print("\n[7] VÉRIFICATION D'INTÉGRITÉ:")

# Vérifier les IDs manquants
print("\n  IDs manquants dans MongoDB vs SQLite:")

for sql_table, mongo_col in [('movies', 'movies'), ('persons', 'persons')]:
    if mongo_col in collections:
        print(f"\n  → {sql_table}:")
        
        # IDs dans SQLite
        sqlite_cursor.execute(f"SELECT mid FROM {sql_table} LIMIT 100")
        sqlite_ids = set(row[0] for row in sqlite_cursor.fetchall())
        
        # IDs dans MongoDB
        mongo_ids = set(doc.get('mid' if sql_table == 'movies' else 'pid') 
                       for doc in mongo_db[mongo_col].find().limit(100))
        
        missing_in_mongo = sqlite_ids - mongo_ids
        missing_in_sqlite = mongo_ids - sqlite_ids
        
        print(f"    IDs SQLite non trouvés dans MongoDB: {len(missing_in_mongo)}")
        if missing_in_mongo:
            print(f"      Exemples: {list(missing_in_mongo)[:5]}")
        
        print(f"    IDs MongoDB non trouvés dans SQLite: {len(missing_in_sqlite)}")
        if missing_in_sqlite:
            print(f"      Exemples: {list(missing_in_sqlite)[:5]}")

# 8. Rapport final
print("\n" + "=" * 60)
print("RAPPORT FINAL")
print("=" * 60)

# Calcul du score d'import
total_tables = len([t for t in tables_to_check if t[1] in collections])
print(f"\nCollections importées: {total_tables}/{len(tables_to_check)}")

if total_tables == len(tables_to_check):
    print("✅ Toutes les collections sont importées")
elif total_tables >= len(tables_to_check) * 0.7:
    print("⚠️  La plupart des collections sont importées")
else:
    print("❌ Import incomplet")

# Nettoyage
sqlite_conn.close()
mongo_client.close()

print("\n✅ Vérification terminée")
import sqlite3
from pymongo import MongoClient, errors
import time
from typing import List, Dict, Any
from tqdm import tqdm  # Pour une barre de progression

def migrate_sqlite_to_mongodb_flat(batch_size: int = 10000) -> Dict[str, Any]:
    """
    Migre toutes les tables SQLite vers MongoDB en collections plates.
    
    Args:
        batch_size: Nombre de documents à insérer par lot (optimisation mémoire)
    
    Returns:
        Dictionnaire avec statistiques et status
    """
    
    stats = {
        "tables_migrated": 0,
        "total_documents": 0,
        "failed_tables": [],
        "execution_time": 0
    }
    
    start_time = time.time()
    
    try:
        # Connexions avec gestion d'erreur
        sqlite_conn = sqlite3.connect('./data/imdb.db')
        mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        
        # Tester la connexion MongoDB
        mongo_client.admin.command('ping')
        
        db = mongo_client['imdb_flat']
        
        # Lister les tables SQLite (exclure les tables système)
        cursor = sqlite_conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"🎯 Migration de {len(tables)} tables vers MongoDB...")
        print("="*60)
        
        for table_name in tqdm(tables, desc="Tables", unit="table"):
            try:
                print(f"\n📊 Table: {table_name}")
                
                # 1. Nettoyer la collection existante
                if table_name in db.list_collection_names():
                    db[table_name].drop()
                    print(f"   ♻️  Collection existante nettoyée")
                
                # 2. Récupérer le schéma pour info
                cursor.execute(f"PRAGMA table_info({table_name})")
                schema = cursor.fetchall()
                print(f"   📋 Schéma: {len(schema)} colonnes")
                
                # 3. Compter les lignes pour la progression
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_rows = cursor.fetchone()[0]
                
                if total_rows == 0:
                    print(f"   ⚠️  Table vide, ignorée")
                    continue
                
                # 4. Récupérer les données par batch
                offset = 0
                inserted_count = 0
                
                cursor.execute(f"SELECT * FROM {table_name}")
                columns = [desc[0] for desc in cursor.description]
                
                with tqdm(total=total_rows, desc=f"  Documents", unit="doc", leave=False) as pbar:
                    while True:
                        cursor.execute(f"""
                            SELECT * FROM {table_name} 
                            LIMIT ? OFFSET ?
                        """, (batch_size, offset))
                        
                        batch = cursor.fetchall()
                        if not batch:
                            break
                        
                        # Conversion en documents MongoDB
                        documents = []
                        for row in batch:
                            doc = {}
                            for i, col in enumerate(columns):
                                value = row[i]
                                # Gestion des types spéciaux
                                if value is None:
                                    doc[col] = None
                                elif isinstance(value, bytes):
                                    try:
                                        doc[col] = value.decode('utf-8')
                                    except:
                                        doc[col] = str(value)
                                elif isinstance(value, (int, float, str, bool)):
                                    doc[col] = value
                                else:
                                    doc[col] = str(value)
                            documents.append(doc)
                        
                        # Insertion par lot
                        if documents:
                            try:
                                result = db[table_name].insert_many(documents, ordered=False)
                                inserted_count += len(result.inserted_ids)
                            except errors.BulkWriteError as e:
                                print(f"   ⚠️  Erreurs d'insertion (continuing): {len(e.details['writeErrors'])}")
                                # On continue avec les documents valides
                        
                        offset += batch_size
                        pbar.update(len(batch))
                
                # 5. Créer un index sur l'ID si la colonne existe
                if 'id' in columns or f'{table_name[:-1]}_id' in ''.join(columns):
                    id_field = next((col for col in columns if col.endswith('_id')), columns[0])
                    db[table_name].create_index([(id_field, 1)])
                    print(f"   🔍 Index créé sur: {id_field}")
                
                print(f"   ✅ {inserted_count:,} documents insérés")
                
                # 6. Vérification
                mongo_count = db[table_name].estimated_document_count()
                if mongo_count == total_rows:
                    print(f"   ✓ Vérification OK: {mongo_count:,} = {total_rows:,}")
                else:
                    print(f"   ⚠️  Écart: MongoDB={mongo_count:,}, SQLite={total_rows:,}")
                
                stats["tables_migrated"] += 1
                stats["total_documents"] += inserted_count
                
                # 7. Afficher un échantillon
                if inserted_count > 0:
                    sample = db[table_name].find_one()
                    print(f"   📄 Exemple: {list(sample.keys())[:5]}...")
            
            except Exception as e:
                print(f"   ❌ Erreur sur table {table_name}: {e}")
                stats["failed_tables"].append((table_name, str(e)))
                continue
        
        # Statistiques finales
        stats["execution_time"] = time.time() - start_time
        
        print("\n" + "="*60)
        print("📈 RAPPORT DE MIGRATION COMPLET")
        print("="*60)
        
        for table_name in tables:
            if table_name in db.list_collection_names():
                count = db[table_name].estimated_document_count()
                print(f"{table_name:25} : {count:>12,} documents")
        
        print("\n" + "="*60)
        print(f"✅ Migration terminée!")
        print(f"⏱️  Temps total: {stats['execution_time']:.2f} secondes")
        print(f"📊 Tables migrées: {stats['tables_migrated']}/{len(tables)}")
        print(f"📄 Documents totaux: {stats['total_documents']:,}")
        
        if stats["failed_tables"]:
            print(f"⚠️  Tables en échec: {len(stats['failed_tables'])}")
            for table, error in stats["failed_tables"]:
                print(f"   - {table}: {error}")
    
    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite: {e}")
        return None
    except errors.ServerSelectionTimeoutError:
        print("❌ MongoDB non accessible. Lancez-le avec: mongod --dbpath ./data/mongo/standalone")
        return None
    finally:
        if 'sqlite_conn' in locals():
            sqlite_conn.close()
        if 'mongo_client' in locals():
            mongo_client.close()
    
    return stats

if __name__ == "__main__":
    # Configuration
    BATCH_SIZE = 50000  # Ajuster selon la RAM disponible
    
    print("🚀 DÉMARRAGE DE LA MIGRATION SQLite → MongoDB")
    print("="*60)
    
    result = migrate_sqlite_to_mongodb_flat(batch_size=BATCH_SIZE)
    
    if result:
        print("\n" + "🎯 MIGRATION RÉUSSIE!")
    else:
        print("\n" + "❌ MIGRATION ÉCHOUÉE")
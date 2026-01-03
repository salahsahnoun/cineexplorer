#!/usr/bin/env python3
# import_data.py - Import COMPLET et OPTIMISÉ des données IMDB vers Replica Set

from pymongo import MongoClient, errors
import time
from datetime import datetime
from typing import List, Dict, Any
import sys
from pathlib import Path

def setup_logging():
    """Configure le logging pour le script"""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('import_replica.log')
        ]
    )
    return logging.getLogger(__name__)

class ReplicaSetImporter:
    """Classe pour gérer l'import vers le Replica Set"""
    
    def __init__(self, replica_set_uri: str = 'localhost:27017,localhost:27018,localhost:27019',
                 source_uri: str = 'localhost:27017'):
        self.logger = setup_logging()
        self.replica_set_uri = replica_set_uri
        self.source_uri = source_uri
        self.replica_client = None
        self.source_client = None
        self.replica_db = None
        self.source_db = None
        self.primary_port = None
        
    def connect_to_replicaset(self) -> bool:
        """Se connecter au Replica Set et trouver le Primary"""
        try:
            self.logger.info("🔗 Connexion au Replica Set...")
            
            # Configuration pour la haute disponibilité
            self.replica_client = MongoClient(
                self.replica_set_uri,
                replicaSet='rs0',
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=60000,
                maxPoolSize=50,
                readPreference='primaryPreferred'
            )
            
            # Vérifier la connexion avec un ping
            self.replica_client.admin.command('ping')
            self.logger.info("✅ Connecté au Replica Set")
            
            # Trouver le Primary
            is_master = self.replica_client.admin.command('isMaster')
            self.primary_port = is_master.get('primary', '').split(':')[-1] if is_master.get('primary') else '27017'
            self.logger.info(f"👑 Primary détecté sur port: {self.primary_port}")
            
            # Utiliser la base imdb_replica
            self.replica_db = self.replica_client['imdb_replica']
            
            return True
            
        except errors.ServerSelectionTimeoutError as e:
            self.logger.error(f"❌ Timeout connexion Replica Set: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Erreur connexion Replica Set: {e}")
            return False
    
    def connect_to_source(self) -> bool:
        """Se connecter à la source imdb_flat"""
        try:
            self.logger.info("🔗 Connexion à la source imdb_flat...")
            
            self.source_client = MongoClient(
                self.source_uri,
                serverSelectionTimeoutMS=15000,
                connectTimeoutMS=15000
            )
            
            # Vérifier que imdb_flat existe
            if 'imdb_flat' not in self.source_client.list_database_names():
                self.logger.error("❌ Base 'imdb_flat' non trouvée!")
                self.logger.info("   Exécutez d'abord: python3 scripts/phase2_mongodb/migrate_flat.py")
                return False
            
            self.source_db = self.source_client['imdb_flat']
            self.logger.info("✅ Connecté à la source imdb_flat")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erreur connexion source: {e}")
            return False
    
    def get_source_statistics(self) -> Dict[str, int]:
        """Récupérer les statistiques de la source"""
        self.logger.info("\n📊 STATISTIQUES DE LA SOURCE:")
        
        collections = [
            'movies', 'persons', 'ratings', 'genres',
            'directors', 'writers', 'principals', 'characters',
            'titles', 'knownformovies', 'professions'
        ]
        
        stats = {}
        for coll_name in collections:
            try:
                if coll_name in self.source_db.list_collection_names():
                    count = self.source_db[coll_name].estimated_document_count()
                    stats[coll_name] = count
                    self.logger.info(f"   {coll_name:20} : {count:>9,} documents")
                else:
                    self.logger.warning(f"   {coll_name:20} : NON TROUVÉE")
                    stats[coll_name] = 0
            except Exception as e:
                self.logger.error(f"   {coll_name:20} : ERREUR - {e}")
                stats[coll_name] = 0
        
        return stats
    
    def import_collection(self, collection_name: str, source_stats: Dict[str, int]) -> tuple:
        """Importer une collection spécifique"""
        if collection_name not in self.source_db.list_collection_names():
            self.logger.warning(f"⚠️  {collection_name} : Collection non trouvée dans la source")
            return 0, False
        
        source_count = source_stats.get(collection_name, 0)
        if source_count == 0:
            self.logger.warning(f"⚠️  {collection_name} : Collection vide")
            return 0, False
        
        self.logger.info(f"\n📄 IMPORT DE: {collection_name.upper()}")
        self.logger.info(f"   📊 Source: {source_count:,} documents")
        
        # Supprimer l'ancienne collection si elle existe
        if collection_name in self.replica_db.list_collection_names():
            try:
                self.replica_db[collection_name].drop()
                self.logger.info("   ♻️  Ancienne collection nettoyée")
            except Exception as e:
                self.logger.error(f"   ❌ Erreur nettoyage: {e}")
        
        # Paramètres d'import
        batch_size = 50000  # Taille optimisée pour MongoDB
        total_imported = 0
        offset = 0
        start_time = time.time()
        
        try:
            # Créer un curseur avec projection pour optimiser
            cursor = self.source_db[collection_name].find(
                {}, 
                batch_size=batch_size,
                no_cursor_timeout=True
            )
            
            current_batch = []
            batch_number = 0
            
            for doc in cursor:
                # Préparer le document
                if '_id' in doc:
                    # Garder l'_id original sauf si c'est un ObjectId
                    if isinstance(doc['_id'], dict) and '$oid' in doc['_id']:
                        # C'est un ObjectId JSON, le convertir
                        from bson import ObjectId
                        doc['_id'] = ObjectId(doc['_id']['$oid'])
                    # Sinon, garder l'_id tel quel
                
                current_batch.append(doc)
                
                # Insérer par lots
                if len(current_batch) >= batch_size:
                    batch_number += 1
                    inserted = self._insert_batch(collection_name, current_batch, batch_number)
                    total_imported += inserted
                    current_batch = []
                    
                    # Afficher progression
                    if batch_number % 10 == 0:
                        elapsed = time.time() - start_time
                        docs_per_sec = total_imported / elapsed if elapsed > 0 else 0
                        self.logger.info(f"   ↳ Lot {batch_number}: {total_imported:,} / {source_count:,} "
                                        f"({(total_imported/source_count*100):.1f}%) - "
                                        f"{docs_per_sec:.0f} doc/s")
            
            # Insérer le dernier lot
            if current_batch:
                batch_number += 1
                inserted = self._insert_batch(collection_name, current_batch, batch_number)
                total_imported += inserted
            
            cursor.close()
            
            elapsed = time.time() - start_time
            docs_per_sec = total_imported / elapsed if elapsed > 0 else 0
            
            self.logger.info(f"   ✅ {total_imported:,} documents importés en {elapsed:.2f}s "
                           f"({docs_per_sec:.0f} doc/s)")
            
            return total_imported, True
            
        except Exception as e:
            self.logger.error(f"   ❌ Erreur import {collection_name}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return total_imported, False
    
    def _insert_batch(self, collection_name: str, batch: List[Dict], batch_number: int) -> int:
        """Insérer un lot de documents avec gestion d'erreurs"""
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                result = self.replica_db[collection_name].insert_many(
                    batch,
                    ordered=False,  # Continue en cas d'erreur
                    bypass_document_validation=False
                )
                return len(result.inserted_ids)
                
            except errors.BulkWriteError as bwe:
                # Gestion des erreurs d'insertion
                inserted = len(bwe.details.get('insertedIds', []))
                self.logger.warning(f"   ⚠️  Lot {batch_number}: {len(batch)-inserted} erreurs "
                                  f"(attempt {attempt+1}/{max_retries})")
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    return inserted
                    
            except Exception as e:
                self.logger.error(f"   ❌ Erreur lot {batch_number}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    return 0
    
    def verify_replication(self, imported_collections: List[str]) -> bool:
        """Vérifier que la réplication fonctionne"""
        self.logger.info("\n🔍 VÉRIFICATION DE LA RÉPLICATION:")
        
        if not imported_collections:
            self.logger.warning("⚠️  Aucune collection importée à vérifier")
            return False
        
        # Attendre que la réplication se stabilise
        self.logger.info("   ⏳ Attente stabilisation réplication (10s)...")
        time.sleep(10)
        
        sample_collection = 'movies' if 'movies' in imported_collections else imported_collections[0]
        
        try:
            # Récupérer le compte depuis le Primary
            primary_count = self.replica_db[sample_collection].estimated_document_count()
            
            # Se connecter à un Secondary pour vérifier
            secondary_port = '27018' if self.primary_port != '27018' else '27019'
            secondary_client = MongoClient(
                f'localhost:{secondary_port}',
                serverSelectionTimeoutMS=10000,
                readPreference='secondary'
            )
            
            # Activer la lecture sur secondary
            secondary_client.admin.command('ping')
            secondary_db = secondary_client['imdb_replica']
            
            # Essayer plusieurs fois (la réplication peut être en retard)
            max_attempts = 5
            secondary_count = 0
            
            for attempt in range(max_attempts):
                try:
                    secondary_count = secondary_db[sample_collection].estimated_document_count()
                    if secondary_count == primary_count:
                        break
                    else:
                        self.logger.info(f"   ⏳ Attente réplication... "
                                       f"Primary: {primary_count:,}, Secondary: {secondary_count:,}")
                        time.sleep(5)
                except:
                    time.sleep(2)
            
            secondary_client.close()
            
            if secondary_count == primary_count:
                self.logger.info(f"   ✅ Réplication OK: {sample_collection} = {primary_count:,} documents")
                return True
            else:
                self.logger.warning(f"   ⚠️  Réplication incomplète: "
                                  f"Primary={primary_count:,}, Secondary={secondary_count:,}")
                return False
                
        except Exception as e:
            self.logger.error(f"   ❌ Erreur vérification réplication: {e}")
            return False
    
    def run_import(self) -> bool:
        """Exécuter l'import complet"""
        start_time = time.time()
        
        self.logger.info("=" * 70)
        self.logger.info("📥 IMPORT COMPLET DES DONNÉES IMDB VERS LE REPLICA SET")
        self.logger.info("=" * 70)
        
        try:
            # 1. Connexions
            if not self.connect_to_replicaset():
                return False
            if not self.connect_to_source():
                return False
            
            # 2. Statistiques source
            source_stats = self.get_source_statistics()
            
            # 3. Import des collections
            self.logger.info("\n🚀 DÉBUT DE L'IMPORT:")
            
            collections = [
                'movies', 'persons', 'ratings', 'genres',
                'directors', 'writers', 'principals', 'characters',
                'titles', 'knownformovies', 'professions'
            ]
            
            total_docs = 0
            imported_collections = []
            failed_collections = []
            
            for collection_name in collections:
                imported_count, success = self.import_collection(collection_name, source_stats)
                
                if success and imported_count > 0:
                    total_docs += imported_count
                    imported_collections.append(collection_name)
                elif not success:
                    failed_collections.append(collection_name)
                
                # Petite pause entre les collections
                time.sleep(1)
            
            # 4. Rapport final
            elapsed_time = time.time() - start_time
            
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📈 RAPPORT D'IMPORT FINAL")
            self.logger.info("=" * 70)
            
            self.logger.info(f"⏱️  Temps total: {elapsed_time:.2f} secondes")
            self.logger.info(f"📦 Collections importées: {len(imported_collections)}/{len(collections)}")
            self.logger.info(f"📄 Documents totaux: {total_docs:,}")
            
            if failed_collections:
                self.logger.warning(f"⚠️  Collections en échec: {', '.join(failed_collections)}")
            
            # 5. Vérification détaillée
            self.logger.info("\n📊 VÉRIFICATION DÉTAILLÉE:")
            for coll_name in imported_collections:
                try:
                    replica_count = self.replica_db[coll_name].estimated_document_count()
                    source_count = source_stats.get(coll_name, 0)
                    
                    if replica_count == source_count:
                        status = "✅"
                    else:
                        status = "⚠️"
                    
                    self.logger.info(f"{status} {coll_name:20} : {replica_count:>9,} / {source_count:>9,}")
                    
                except Exception as e:
                    self.logger.error(f"❌ {coll_name:20} : ERREUR - {e}")
            
            # 6. Vérification réplication
            replication_ok = self.verify_replication(imported_collections)
            
            # 7. Nettoyage et fermeture
            if self.source_client:
                self.source_client.close()
            if self.replica_client:
                self.replica_client.close()
            
            self.logger.info("\n" + "=" * 70)
            
            if len(imported_collections) > 0 and replication_ok:
                self.logger.info("🎉 IMPORT TERMINÉ AVEC SUCCÈS !")
                self.logger.info("=" * 70)
                return True
            else:
                self.logger.warning("⚠️  IMPORT TERMINÉ AVEC DES WARNINGS")
                self.logger.info("=" * 70)
                return False
                
        except KeyboardInterrupt:
            self.logger.warning("\n⏹️ Import interrompu par l'utilisateur")
            return False
        except Exception as e:
            self.logger.error(f"\n❌ ERREUR CRITIQUE: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

def main():
    """Fonction principale"""
    # Vérifier que MongoDB tourne
    try:
        test_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        test_client.admin.command('ping')
        test_client.close()
    except:
        print("❌ MongoDB n'est pas accessible sur localhost:27017")
        print("   Démarrez MongoDB avec: mongod --dbpath ./data/mongo/standalone")
        return False
    
    # Créer et exécuter l'import
    importer = ReplicaSetImporter()
    success = importer.run_import()
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
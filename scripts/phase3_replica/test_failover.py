#!/usr/bin/env python3
"""
T3.2 - Tests de tolérance aux pannes MongoDB Replica Set
Version simplifiée qui respecte EXACTEMENT la consigne.

7 tests demandés :
1. État initial - Capturer rs.status(), identifier Primary/Secondary
2. Écriture - Insérer des documents, vérifier la réplication  
3. Panne Primary - Arrêter le Primary (Ctrl+C), observer l'élection
4. Nouveau Primary - Mesurer le temps d'élection, vérifier les données
5. Lecture - Confirmer que les données sont accessibles
6. Reconnexion - Relancer le nœud arrêté, observer la resync
7. Double panne - Que se passe-t-il si 2 nœuds tombent ?
"""

import time
import json
from datetime import datetime
from pymongo import MongoClient, ReadPreference
import os

# Configuration
PORTS = [27017, 27018, 27019]
REPLICA_SET = "rs0"
TEST_DB = "imdb_replica"
TEST_COLL = "failover_test"

class ReplicaSetTester:
    """Testeur simple pour les 7 tests de tolérance aux pannes"""
    
    def __init__(self):
        self.results = {}
        self.screenshots = []
        self.primary_port = None
        self.primary_name = None
        self.test_start_time = datetime.now()
        
    def log(self, message, emoji=""):
        """Affichage formaté"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if emoji:
            print(f"[{timestamp}] {emoji} {message}")
        else:
            print(f"[{timestamp}] {message}")
    
    def save_screenshot(self, content, filename_prefix):
        """Sauvegarde une capture pour documentation"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join("capture", f"capture_{filename_prefix}_{timestamp}.txt")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Capture: {filename_prefix}\n")
            f.write(f"Heure: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n")
            f.write(content)
            f.write("\n" + "="*60)
        
        self.screenshots.append(filename)
        self.log(f"Capture sauvegardée: {filename}", "📸")
        return filename
    
    # ==================== UTILITAIRES ====================
    
    def get_mongo_client(self, port, read_preference=None):
        """Crée un client MongoDB"""
        if read_preference:
            return MongoClient(
                f"mongodb://localhost:{port}/",
                serverSelectionTimeoutMS=5000,
                read_preference=read_preference
            )
        return MongoClient(f"mongodb://localhost:{port}/", serverSelectionTimeoutMS=5000)
    
    def get_replica_status(self):
        """Récupère le statut du replica set"""
        for port in PORTS:
            try:
                client = self.get_mongo_client(port)
                status = client.admin.command("replSetGetStatus")
                client.close()
                return status
            except:
                continue
        raise Exception("Impossible de se connecter au replica set")
    
    def identify_primary_secondary(self):
        """Identifie Primary et Secondaires"""
        status = self.get_replica_status()
        
        primary = None
        secondaires = []
        
        for member in status['members']:
            if member['stateStr'] == 'PRIMARY':
                primary = member['name']
                self.primary_name = primary
                self.primary_port = int(primary.split(':')[1])
            elif member['stateStr'] == 'SECONDARY':
                secondaires.append(member['name'])
        
        return primary, secondaires, status
    
    # ==================== TESTS ====================
    
    def test_1_etat_initial(self):
        """1. État initial - Capturer rs.status(), identifier Primary/Secondary"""
        self.log("TEST 1: ÉTAT INITIAL", "🧪")
        self.log("="*60)
        
        try:
            # Capturer rs.status()
            status = self.get_replica_status()
            status_json = json.dumps(status, indent=2, default=str)
            
            # Identifier Primary/Secondary
            primary, secondaires, _ = self.identify_primary_secondary()
            
            # Sauvegarder la capture
            capture_content = f"Primary: {primary}\nSecondaires: {secondaires}\n\nrs.status():\n{status_json}"
            screenshot = self.save_screenshot(capture_content, "test1_etat_initial")
            
            self.log(f"Primary identifié: {primary}", "👑")
            self.log(f"Secondaires identifiés: {len(secondaires)}", "🔄")
            
            self.results['test1'] = {
                'status': 'SUCCÈS',
                'primary': primary,
                'secondaires': secondaires,
                'screenshot': screenshot,
                'observation': f"Cluster opérationnel avec {len(secondaires)+1} membres"
            }
            
            return True
            
        except Exception as e:
            self.log(f"Échec: {e}", "❌")
            self.results['test1'] = {'status': 'ÉCHEC', 'erreur': str(e)}
            return False
    
    def test_2_ecriture_replication(self):
        """2. Écriture - Insérer des documents, vérifier la réplication"""
        self.log("\nTEST 2: ÉCRITURE ET VÉRIFICATION RÉPLICATION", "🧪")
        self.log("="*60)
        
        try:
            # Trouver le Primary
            if not self.primary_port:
                primary, _, _ = self.identify_primary_secondary()
                if not primary:
                    raise Exception("Aucun Primary trouvé")
            
            # Insérer des documents
            client = self.get_mongo_client(self.primary_port)
            db = client[TEST_DB]
            collection = db[TEST_COLL]
            
            # Nettoyer les anciens tests
            collection.drop()
            
            # Insérer 3 documents
            documents = []
            for i in range(3):
                doc = {
                    "test_id": f"doc_{i}",
                    "message": "Test écriture T3.2",
                    "timestamp": datetime.now(),
                    "primary": self.primary_name
                }
                documents.append(doc)
            
            result = collection.insert_many(documents)
            self.log(f"{len(documents)} documents insérés sur {self.primary_name}", "✅")
            
            # Attendre réplication
            self.log("Attente réplication (5 secondes)...", "⏳")
            time.sleep(5)
            
            # Vérifier la réplication sur les Secondaires
            replication_ok = True
            for port in PORTS:
                if port != self.primary_port:
                    try:
                        sec_client = self.get_mongo_client(port)
                        count = sec_client[TEST_DB][TEST_COLL].count_documents({})
                        sec_client.close()
                        
                        if count == len(documents):
                            self.log(f"Réplication OK sur port {port}: {count} documents", "✅")
                        else:
                            self.log(f"Réplication incomplète port {port}: {count}/{len(documents)}", "⚠️")
                            replication_ok = False
                    except Exception as e:
                        self.log(f"Erreur port {port}: {e}", "❌")
                        replication_ok = False
            
            client.close()
            
            # Sauvegarder capture
            capture_content = f"Documents insérés: {len(documents)}\n"
            capture_content += f"Réplication vérifiée sur {len(PORTS)-1} port(s)\n"
            capture_content += f"Réplication complète: {'OUI' if replication_ok else 'NON'}"
            
            screenshot = self.save_screenshot(capture_content, "test2_ecriture")
            
            self.results['test2'] = {
                'status': 'SUCCÈS' if replication_ok else 'PARTIEL',
                'documents_insertes': len(documents),
                'replication_complete': replication_ok,
                'screenshot': screenshot,
                'observation': f"Écriture réussie, réplication {'OK' if replication_ok else 'partielle'}"
            }
            
            return replication_ok
            
        except Exception as e:
            self.log(f"Échec: {e}", "❌")
            self.results['test2'] = {'status': 'ÉCHEC', 'erreur': str(e)}
            return False
    
    def test_3_panne_primary(self):
        """3. Panne Primary - Arrêter le Primary (Ctrl+C), observer l'élection"""
        self.log("\nTEST 3: PANNE PRIMARY ET OBSERVATION ÉLECTION", "🧪")
        self.log("="*60)
        
        self.log("⚠️  CE TEST REQUIERT UNE INTERVENTION MANUELLE", "⚠️")
        self.log("Suivez ces étapes:")
        self.log("1. Primary actuel à arrêter:", "👑")
        self.log(f"   → {self.primary_name} (port {self.primary_port})", "   ")
        
        # Instructions détaillées
        instructions = f"PRIMARY À ARRÊTER:\n"
        instructions += f"  Nom: {self.primary_name}\n"
        instructions += f"  Port: {self.primary_port}\n\n"
        instructions += "ÉTAPES MANUELLES:\n"
        instructions += "1. Trouver le terminal avec ce mongod\n"
        instructions += f"   Commande: ps aux | grep 'mongod.*{self.primary_port}'\n"
        instructions += "2. Appuyer sur Ctrl+C dans ce terminal\n"
        instructions += "3. Observer les logs d'élection\n"
        instructions += f"   Commande: tail -f data/mongo/db-{self.primary_port-27016}/mongod.log\n\n"
        instructions += "INDICATEURS D'ÉLECTION DANS LES LOGS:\n"
        instructions += "  - 'ELECTION' messages\n"
        instructions += "  - 'stepDown' ou 'new primary'\n"
        instructions += "  - Changement d'état des membres"
        
        print("\n" + "="*60)
        print("📋 INSTRUCTIONS POUR ARRÊTER LE PRIMARY:")
        print("="*60)
        print(instructions)
        print("="*60)
        
        screenshot = self.save_screenshot(instructions, "test3_instructions")
        
        # Attendre l'action manuelle
        input("\n⏳ Après avoir arrêté le Primary, appuyez sur Entrée...")
        
        self.results['test3'] = {
            'status': 'MANUEL',
            'primary_stoppe': self.primary_name,
            'instructions': screenshot,
            'observation': f"Primary {self.primary_name} arrêté manuellement (Ctrl+C)"
        }
        
        return True
    
    def test_4_nouveau_primary(self):
        """4. Nouveau Primary - Mesurer le temps d'élection, vérifier les données"""
        self.log("\nTEST 4: NOUVEAU PRIMARY", "🧪")
        self.log("="*60)
        
        # Demander le temps mesuré manuellement
        print("⏱️  TEMPS D'ÉLECTION MESURÉ MANUELLEMENT")
        print("-" * 40)
        print("1. Combien de secondes entre le Ctrl+C et l'apparition")
        print("   du nouveau Primary dans les logs?")
        
        try:
            election_time = float(input("   Temps (secondes): ").strip() or "0")
        except:
            election_time = 0
        
        # Identifier le nouveau Primary
        self.log("\n🔍 Identification du nouveau Primary...", "🔍")
        time.sleep(3)  # Laisser le temps
        
        try:
            new_primary, new_secondaires, status = self.identify_primary_secondary()
            
            if new_primary:
                self.log(f"Nouveau Primary: {new_primary}", "👑")
                
                # Vérifier les données
                client = self.get_mongo_client(self.primary_port)
                count = client[TEST_DB][TEST_COLL].count_documents({})
                client.close()
                
                self.log(f"Documents accessibles: {count}", "📊")
                data_status = "OK" if count > 0 else "PROBLÈME"
            else:
                self.log("Aucun Primary trouvé!", "❌")
                new_primary = "Non identifié"
                data_status = "INCONNU"
                
        except Exception as e:
            self.log(f"Erreur: {e}", "❌")
            new_primary = "Erreur"
            data_status = f"Erreur: {e}"
        
        # Capture des résultats
        results = f"TEMPS D'ÉLECTION: {election_time} secondes\n"
        results += f"NOUVEAU PRIMARY: {new_primary}\n"
        results += f"DONNÉES ACCESSIBLES: {data_status}\n"
        results += f"NOMBRE DE DOCUMENTS: {count if 'count' in locals() else 'N/A'}"
        
        screenshot = self.save_screenshot(results, "test4_nouveau_primary")
        
        self.results['test4'] = {
            'status': 'MANUEL',
            'temps_election': election_time,
            'nouveau_primary': new_primary,
            'donnees_accessibles': data_status,
            'screenshot': screenshot,
            'observation': f"Élection: {election_time}s, Données: {data_status}"
        }
        
        return True
    
    def test_5_lecture(self):
        """5. Lecture - Confirmer que les données sont accessibles"""
        self.log("\nTEST 5: LECTURE DES DONNÉES", "🧪")
        self.log("="*60)
        
        try:
            # Trouver un Secondary
            _, secondaires, _ = self.identify_primary_secondary()
            
            if not secondaires:
                self.log("Aucun Secondary disponible", "❌")
                return False
            
            # Utiliser le premier Secondary
            secondary_port = int(secondaires[0].split(':')[1])
            
            # Tenter la lecture
            self.log(f"Lecture depuis Secondary: {secondaires[0]}", "📖")
            
            client = self.get_mongo_client(secondary_port, ReadPreference.SECONDARY)
            db = client[TEST_DB]
            
            # Compter les documents
            count = db[TEST_COLL].count_documents({})
            self.log(f"Documents accessibles: {count}", "✅")
            
            # Lire un échantillon
            if count > 0:
                sample = db[TEST_COLL].find_one()
                self.log(f"Échantillon: {sample.get('test_id', 'N/A')}", "📄")
            
            client.close()
            
            # Capture
            capture_content = f"LECTURE DEPUIS SECONDARY: {secondaires[0]}\n"
            capture_content += f"DOCUMENTS ACCESSIBLES: {count}\n"
            capture_content += f"TEST RÉUSSI: {'OUI' if count > 0 else 'NON'}"
            
            screenshot = self.save_screenshot(capture_content, "test5_lecture")
            
            self.results['test5'] = {
                'status': 'SUCCÈS' if count > 0 else 'PARTIEL',
                'secondary': secondaires[0],
                'documents_accessibles': count,
                'screenshot': screenshot,
                'observation': f"Données accessibles depuis Secondary: {count} documents"
            }
            
            return count > 0
            
        except Exception as e:
            self.log(f"Erreur: {e}", "❌")
            self.results['test5'] = {'status': 'ÉCHEC', 'erreur': str(e)}
            return False
    
    def test_6_reconnexion(self):
        """6. Reconnexion - Relancer le nœud arrêté, observer la resync"""
        self.log("\nTEST 6: RECONNEXION ET RESYNCHRONISATION", "🧪")
        self.log("="*60)
        
        self.log("⚠️  CE TEST REQUIERT UNE INTERVENTION MANUELLE", "⚠️")
        
        # Identifier le nœud arrêté (celui du test 3)
        old_primary = self.results.get('test3', {}).get('primary_stoppe', '27017')
        old_port = int(old_primary.split(':')[1]) if ':' in old_primary else 27017
        
        instructions = f"NŒUD À REDÉMARRER:\n"
        instructions += f"  Nom: {old_primary}\n"
        instructions += f"  Port: {old_port}\n"
        instructions += f"  Répertoire: data/mongo/db-{old_port-27016}\n\n"
        instructions += "COMMANDE DE REDÉMARRAGE:\n"
        instructions += f"  mongod --replSet {REPLICA_SET} \\\n"
        instructions += f"    --port {old_port} \\\n"
        instructions += f"    --dbpath ./data/mongo/db-{old_port-27016} \\\n"
        instructions += f"    --bind_ip localhost --fork \\\n"
        instructions += f"    --logpath ./data/mongo/mongod-{old_port}.log\n\n"
        instructions += "POUR OBSERVER LA RESYNC:\n"
        instructions += f"  tail -f logs/mongod-{old_port}.log\n"
        instructions += "  Rechercher: 'resync', 'initial sync', 'catchup'"
        
        print("\n" + "="*60)
        print("📋 INSTRUCTIONS POUR RECONNEXION:")
        print("="*60)
        print(instructions)
        print("="*60)
        
        screenshot = self.save_screenshot(instructions, "test6_instructions")
        
        # Attendre l'action manuelle
        input("\n⏳ Après avoir redémarré et observé la resync, appuyez sur Entrée...")
        
        self.results['test6'] = {
            'status': 'MANUEL',
            'node_redemarre': old_primary,
            'instructions': screenshot,
            'observation': f"Nœud {old_primary} redémarré, resync observée"
        }
        
        return True
    
    def test_7_double_panne(self):
        """7. Double panne - Que se passe-t-il si 2 nœuds tombent ?"""
        self.log("\nTEST 7: DOUBLE PANNE", "🧪")
        self.log("="*60)
        
        self.log("🔍 SCÉNARIO: 2 nœuds sur 3 arrêtés", "🔍")
        self.log("Cette situation provoque la perte de quorum", "💡")
        
        scenario = "SCÉNARIO DOUBLE PANNE:\n"
        scenario += "  • Arrêter 2 nœuds mongod\n"
        scenario += "  • Il ne reste qu'1 nœud actif\n"
        scenario += "  • Plus de majorité (2/3) disponible\n\n"
        scenario += "COMPORTEMENT ATTENDU:\n"
        scenario += "  • Pas d'élection possible\n"
        scenario += "  • Cluster en mode lecture seule\n"
        scenario += "  • Écritures impossibles\n"
        scenario += "  • Lectures possibles (si nœud restant actif)\n\n"
        scenario += "QUESTIONS À OBSERVER:\n"
        scenario += "  1. Combien de temps pour détecter la perte de quorum?\n"
        scenario += "  2. Les lectures fonctionnent-elles?\n"
        scenario += "  3. Que se passe-t-il si on tente d'écrire?\n"
        scenario += "  4. Comment le cluster réagit-il?"
        
        print("\n" + "="*60)
        print("📋 SCÉNARIO DOUBLE PANNE:")
        print("="*60)
        print(scenario)
        print("="*60)
        
        screenshot = self.save_screenshot(scenario, "test7_scenario")
        
        # Demander les observations
        print("\n📝 APRÈS AVOIR TESTÉ MANUELLEMENT CE SCÉNARIO,")
        print("   notez vos observations ci-dessous:")
        print("-" * 40)
        
        observations = input("Vos observations: ").strip()
        
        self.results['test7'] = {
            'status': 'MANUEL',
            'scenario': 'Double panne - 2 nœuds arrêtés',
            'observations': observations,
            'screenshot': screenshot,
            'observation': "Test manuel de double panne effectué"
        }
        
        return True
    
    def generer_rapport(self):
        """Génère un rapport complet"""
        self.log("\n" + "="*70, "📋")
        self.log("RAPPORT COMPLET DES TESTS T3.2", "📋")
        self.log("="*70, "📋")
        
        # Statistiques
        total_tests = 7
        tests_success = sum(1 for r in self.results.values() if r.get('status') == 'SUCCÈS')
        tests_manual = sum(1 for r in self.results.values() if r.get('status') == 'MANUEL')
        tests_partial = sum(1 for r in self.results.values() if r.get('status') == 'PARTIEL')
        tests_failed = sum(1 for r in self.results.values() if r.get('status') == 'ÉCHEC')
        
        print(f"\n📊 STATISTIQUES:")
        print(f"   Tests réalisés: {total_tests}")
        print(f"   ✅ Tests automatiques réussis: {tests_success}")
        print(f"   👤 Tests manuels guidés: {tests_manual}")
        print(f"   ⚠️  Tests partiels: {tests_partial}")
        print(f"   ❌ Tests échoués: {tests_failed}")
        
        print(f"\n📁 CAPTURES GÉNÉRÉES ({len(self.screenshots)} fichiers):")
        for screenshot in self.screenshots:
            print(f"   • {screenshot}")
        
        print(f"\n🧾 DÉTAIL PAR TEST:")
        print("-" * 70)
        
        test_descriptions = {
            'test1': "1. État initial",
            'test2': "2. Écriture/réplication", 
            'test3': "3. Panne Primary",
            'test4': "4. Nouveau Primary",
            'test5': "5. Lecture",
            'test6': "6. Reconnexion",
            'test7': "7. Double panne"
        }
        
        for test_key, description in test_descriptions.items():
            if test_key in self.results:
                result = self.results[test_key]
                status = result.get('status', 'INCOMPLET')
                
                if status == 'SUCCÈS':
                    icon = "✅"
                elif status == 'MANUEL':
                    icon = "👤"
                elif status == 'PARTIEL':
                    icon = "⚠️ "
                else:
                    icon = "❌"
                
                print(f"\n{icon} {description}: {status}")
                
                if 'observation' in result:
                    print(f"   Observation: {result['observation']}")
        
        # Sauvegarder rapport JSON
        report_data = {
            'projet': 'T3.2 - Tests tolérance aux pannes',
            'date': self.test_start_time.isoformat(),
            'duree': str(datetime.now() - self.test_start_time),
            'resultats': self.results,
            'captures': self.screenshots
        }
        
        report_file = os.path.join("capture", "t3_2_tests_rapport.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)
        
        self.log(f"\n💾 Rapport JSON sauvegardé: {report_file}", "💾")
        
        
        print(f"\n" + "="*70)
        print("🎉 TESTS T3.2 TERMINÉS")
        print("="*70)
    
    def executer_tous_tests(self):
        """Exécute les 7 tests dans l'ordre"""
        print("="*70)
        print("🚀 TESTS DE TOLÉRANCE AUX PANNES - T3.2")
        print("="*70)
        print("Ce script guide l'exécution des 7 tests demandés:")
        print("  • Tests 1, 2, 5: Automatiques")
        print("  • Tests 3, 4, 6, 7: Guidés (actions manuelles)")
        print("="*70)
        
        # Vérification initiale
        print("\n🔍 Vérification de la connexion MongoDB...")
        try:
            status = self.get_replica_status()
            print("✅ MongoDB Replica Set accessible")
        except Exception as e:
            print(f"❌ Erreur: {e}")
            print("   Vérifiez que MongoDB tourne sur les ports 27017, 27018, 27019")
            return
        
        # Exécution des tests
        tests = [
            (self.test_1_etat_initial, "Test 1: État initial"),
            (self.test_2_ecriture_replication, "Test 2: Écriture/réplication"),
            (self.test_3_panne_primary, "Test 3: Panne Primary"),
            (self.test_4_nouveau_primary, "Test 4: Nouveau Primary"),
            (self.test_5_lecture, "Test 5: Lecture"),
            (self.test_6_reconnexion, "Test 6: Reconnexion"),
            (self.test_7_double_panne, "Test 7: Double panne")
        ]
        
        for i, (test_func, description) in enumerate(tests, 1):
            print(f"\n▶️  Exécution {i}/7: {description}")
            
            try:
                test_func()
                print(f"   Statut: {self.results[f'test{i}'].get('status', 'INCOMPLET')}")
                
                if i < len(tests):
                    print("\n⏸️  Pause avant le test suivant...")
                    time.sleep(2)
                    
            except KeyboardInterrupt:
                print("\n⏹️  Tests interrompus par l'utilisateur")
                break
            except Exception as e:
                print(f"❌ Exception: {e}")
        
        # Générer le rapport
        self.generer_rapport()

# Point d'entrée principal
if __name__ == "__main__":
    print("🔧 T3.2 - Tests de tolérance aux pannes MongoDB")
    print("   Version simplifiée respectant exactement la consigne")
    print()
    
    tester = ReplicaSetTester()
    tester.executer_tous_tests()
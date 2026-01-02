#!/bin/bash
# setup_replica.sh - Configuration automatique du Replica Set MongoDB

echo "============================================================"
echo "🚀 CONFIGURATION AUTOMATIQUE DU REPLICA SET - Phase 3"
echo "============================================================"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MONGO_DIR="$BASE_DIR/data/mongo"

# 1. Arrêter les instances existantes
echo "🛑 Arrêt des instances MongoDB existantes..."
pkill -9 mongod 2>/dev/null
sleep 5

# 2. Nettoyer et créer les répertoires
echo "📁 Nettoyage des répertoires de données..."
rm -rf "$MONGO_DIR/db-1" "$MONGO_DIR/db-2" "$MONGO_DIR/db-3" 2>/dev/null
mkdir -p "$MONGO_DIR/db-1" "$MONGO_DIR/db-2" "$MONGO_DIR/db-3"

# 3. Démarrer les instances SANS --fork (pour voir les logs)
echo "🚀 Démarrage des 3 instances MongoDB (sans fork)..."
echo "⚠️  Ouvre 3 terminaux séparés et exécute:"
echo ""
echo "Terminal 1:"
echo "  mongod --replSet rs0 --port 27017 --dbpath $MONGO_DIR/db-1 --bind_ip localhost"
echo ""
echo "Terminal 2:"
echo "  mongod --replSet rs0 --port 27018 --dbpath $MONGO_DIR/db-2 --bind_ip localhost"
echo ""
echo "Terminal 3:"
echo "  mongod --replSet rs0 --port 27019 --dbpath $MONGO_DIR/db-3 --bind_ip localhost"
echo ""
echo "⏳ Attends que les 3 affichent: 'waiting for connections on port...'"
echo "Puis passe à l'étape suivante."
echo ""
read -p "✅ Les 3 instances sont démarrées ? (Appuie sur Entrée) "

# 4. Initialiser le Replica Set
echo "⚙️  Initialisation du Replica Set..."
mongosh --port 27017 --quiet --eval "
print('⏳ Attente que MongoDB soit prêt...');
sleep(5000);

try {
    print('Initialisation du Replica Set...');
    var result = rs.initiate({
        _id: 'rs0',
        members: [
            { _id: 0, host: 'localhost:27017' },
            { _id: 1, host: 'localhost:27018' },
            { _id: 2, host: 'localhost:27019' }
        ]
    });
    
    if (result.ok === 1) {
        print('✅ Replica Set initialisé avec succès');
        print('⏳ Attente de l\\'élection du Primary (peut prendre 30-60s)...');
        
        // Attendre l'élection
        for (var i = 0; i < 12; i++) {
            sleep(5000);
            var status = rs.status();
            var primary = status.members.find(function(m) { 
                return m.stateStr === 'PRIMARY'; 
            });
            
            if (primary) {
                print('🎉 Primary élu: ' + primary.name);
                print('📊 Secondaires: ' + status.members.filter(function(m) {
                    return m.stateStr === 'SECONDARY';
                }).length);
                break;
            }
            
            if (i < 11) {
                print('⏳ Attente élection... ' + ((i+1)*5) + 's');
            } else {
                print('⚠️  Aucun Primary élu après 60s');
            }
        }
    } else {
        print('❌ Erreur initialisation: ' + JSON.stringify(result));
    }
} catch (e) {
    print('❌ Erreur: ' + e.message);
}
"

# 5. Vérifier le statut
echo "📊 Vérification du statut final..."
mongosh --port 27017 --quiet --eval "
try {
    var status = rs.status();
    print('✅ Replica Set opérationnel');
    
    status.members.forEach(function(member) {
        var icon = member.health === 1 ? '✅' : '❌';
        var role = member.stateStr === 'PRIMARY' ? '👑 ' : '   ';
        print(icon + role + member.name + ' : ' + member.stateStr);
    });
} catch (e) {
    print('❌ Impossible de récupérer le statut: ' + e.message);
}
"

# 6. Import optionnel
if [ "$1" == "--import" ]; then
    echo "📥 Import des données..."
    python3 -c "
from pymongo import MongoClient
import time

print('Connexion au Replica Set...')
client = MongoClient('localhost:27017,localhost:27018,localhost:27019', 
                     replicaSet='rs0', 
                     serverSelectionTimeoutMS=30000)

# Attendre que le Primary soit disponible
for i in range(10):
    try:
        client.admin.command('ping')
        print('✅ Connecté au Replica Set')
        break
    except:
        print(f'⏳ Tentative {i+1}/10...')
        time.sleep(3)

# Vérifier le Primary
try:
    is_master = client.admin.command('isMaster')
    print(f\"Primary: {is_master.get('primary', 'N/A')}\")
    
    # Importer des données de test
    db = client['imdb_replica']
    db.test.insert_one({'message': 'Test import', 'time': time.time()})
    print('✅ Test d\\'écriture réussi')
    
except Exception as e:
    print(f'❌ Erreur: {e}')

client.close()
"
fi

echo ""
echo "============================================================"
echo "✅ CONFIGURATION TERMINÉE !"
echo "============================================================"
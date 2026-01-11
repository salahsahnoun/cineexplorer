#!/bin/bash
# scripts/phase3_replica/setup_replica.sh - Version améliorée

set -e  # Arrêter sur erreur

echo "========================================="
echo "🚀 CONFIGURATION AUTOMATIQUE REPLICA SET"
echo "========================================="

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MONGO_DIR="$BASE_DIR/data/mongo"

# 1. Arrêt propre des instances
echo "🛑 Arrêt des instances MongoDB du projet..."
if pgrep -f "mongod.*replSet.*rs0" > /dev/null; then
    echo "   Arrêt en cours..."
    pkill -f "mongod.*replSet.*rs0" || true
    sleep 3
    # Kill forcé si nécessaire
    if pgrep -f "mongod.*replSet.*rs0" > /dev/null; then
        echo "   Arrêt forcé..."
        pkill -9 -f "mongod.*replSet.*rs0" || true
    fi
fi

# 2. Nettoyage sélectif des sockets
echo "🧹 Nettoyage des fichiers temporaires..."
rm -f /tmp/mongodb-27017.sock /tmp/mongodb-27018.sock /tmp/mongodb-27019.sock 2>/dev/null || true

# 3. Préparation des répertoires
echo "📁 Création des répertoires de données..."
rm -rf "$MONGO_DIR/db1" "$MONGO_DIR/db2" "$MONGO_DIR/db3" 2>/dev/null || true
mkdir -p "$MONGO_DIR/db1" "$MONGO_DIR/db2" "$MONGO_DIR/db3"

# 4. Lancement des instances
echo "🚀 Lancement des 3 instances MongoDB..."
for port in 27017 27018 27019; do
    db_index=$((port - 27016))
    mongod --replSet rs0 \
           --port $port \
           --dbpath "$MONGO_DIR/db$db_index" \
           --bind_ip localhost \
           --fork \
           --logpath "$MONGO_DIR/db$db_index/mongod.log" \
           --logappend
    echo "   ✅ Instance $port démarrée"
    sleep 2  # Attente entre les démarrages
done

# 5. Attente que les instances soient prêtes
echo "⏳ Attente que MongoDB soit prêt (10s)..."
sleep 10

# 6. Initialisation du Replica Set
echo "⚙️  Initialisation du Replica Set..."
mongosh --port 27017 --quiet --eval "
try {
    print('Initialisation en cours...');
    
    // Vérifier si déjà initialisé
    try {
        var status = rs.status();
        print('⚠️  Replica Set déjà configuré');
    } catch (e) {
        // Pas encore initialisé
        var result = rs.initiate({
            _id: 'rs0',
            members: [
                { _id: 0, host: 'localhost:27017' },
                { _id: 1, host: 'localhost:27018' },
                { _id: 2, host: 'localhost:27019' }
            ]
        });
        
        if (result.ok === 1) {
            print('✅ Replica Set initialisé');
        } else {
            print('❌ Erreur: ' + JSON.stringify(result));
            quit(1);
        }
    }
    
    // Attendre l'élection
    print('⏳ Attente élection Primary (peut prendre 30s)...');
    for (var i = 0; i < 30; i++) {
        sleep(1000);
        try {
            var status = rs.status();
            var primary = status.members.find(m => m.stateStr === 'PRIMARY');
            if (primary) {
                print('🎉 Primary élu: ' + primary.name);
                print('📊 Statut membres:');
                status.members.forEach(m => {
                    print('   ' + (m.health === 1 ? '✅' : '❌') + ' ' + 
                          (m.stateStr === 'PRIMARY' ? '👑 ' : '   ') + 
                          m.name + ' : ' + m.stateStr);
                });
                break;
            }
        } catch(e) {}
        
        if (i === 29) {
            print('⚠️  Élection lente, vérifiez les logs');
        }
    }
    
} catch (error) {
    print('❌ Erreur critique: ' + error.message);
    quit(1);
}
"

# 7. Vérification finale
echo "🔍 Vérification finale..."
mongosh --port 27017 --quiet --eval "
try {
    var status = rs.status();
    print('✅ Replica Set opérationnel');
    print('👑 Primary: ' + (status.members.find(m => m.stateStr === 'PRIMARY')?.name || 'N/A'));
    print('📈 Secondaires: ' + status.members.filter(m => m.stateStr === 'SECONDARY').length);
} catch(e) {
    print('❌ Impossible de vérifier: ' + e.message);
}
"

echo ""
echo "========================================="
echo "✅ CONFIGURATION TERMINÉE !"
echo "========================================="
echo ""
echo "📋 Commandes utiles:"
echo "   mongosh --port 27017          # Se connecter au Primary"
echo "   rs.status()                   # Voir statut Replica Set"
echo "   tail -f data/mongo/db1/mongod.log  # Voir logs"
echo ""
#!/bin/bash
# scripts/phase3_replica/setup_replica.sh - Version robuste

set -e  # Arrêter sur erreur

echo "========================================="
echo "🚀 CONFIGURATION AUTOMATIQUE REPLICA SET"
echo "========================================="

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MONGO_DIR="$BASE_DIR/data/mongo"

# Fonction pour vérifier si un port est utilisé
is_port_used() {
    local port=$1
    lsof -ti:"$port" > /dev/null 2>&1
}

# 1. Arrêt complet de tout MongoDB
echo "🛑 Arrêt complet de toutes les instances MongoDB..."
pkill -f "mongod" || true
sleep 3
pkill -9 -f "mongod" || true
sleep 2

# 2. Nettoyage
echo "🧹 Nettoyage..."
rm -f /tmp/mongodb-*.sock 2>/dev/null || true

# 3. Libération des ports
for port in 27017 27018 27019; do
    if is_port_used "$port"; then
        echo "   Libération du port $port..."
        sudo kill -9 $(sudo lsof -ti:"$port") 2>/dev/null || true
        sleep 1
    fi
done

# 4. Préparation des répertoires
echo "📁 Création des répertoires de données..."
rm -rf "$MONGO_DIR/db-1" "$MONGO_DIR/db-2" "$MONGO_DIR/db-3" 2>/dev/null || true
mkdir -p "$MONGO_DIR/db-1" "$MONGO_DIR/db-2" "$MONGO_DIR/db-3"

# 5. Fonction pour démarrer une instance MongoDB avec replSet
start_mongod_instance() {
    local port=$1
    local db_index=$((port - 27016))
    local db_path="$MONGO_DIR/db-$db_index"
    
    echo "   Démmarage MongoDB sur port $port..."
    
    # Arrêter toute instance existante sur ce port
    if is_port_used "$port"; then
        sudo kill -9 $(sudo lsof -ti:"$port") 2>/dev/null || true
        sleep 1
    fi
    
    # Démarrer MongoDB avec replSet
    mongod --replSet rs0 \
           --port "$port" \
           --dbpath "$db_path" \
           --bind_ip localhost \
           --fork \
           --logpath "$db_path/mongod.log" \
           --pidfilepath "$db_path/mongod.pid"
    
    # Vérifier que le processus est en cours
    if [ -f "$db_path/mongod.pid" ]; then
        local pid=$(cat "$db_path/mongod.pid")
        if ps -p "$pid" > /dev/null; then
            echo "   ✅ Instance $port démarrée (PID: $pid)"
            return 0
        fi
    fi
    
    echo "   ❌ Échec démarrage port $port"
    echo "   Logs:"
    tail -20 "$db_path/mongod.log" 2>/dev/null || echo "   Pas de logs disponibles"
    return 1
}

# 6. Démarrer les instances
echo "🚀 Démarrage des 3 instances MongoDB..."
for port in 27017 27018 27019; do
    if ! start_mongod_instance "$port"; then
        echo "❌ Impossible de démarrer l'instance sur le port $port"
        exit 1
    fi
    sleep 2
done

# 7. Attendre que MongoDB soit prêt
echo "⏳ Attente que MongoDB soit prêt..."
for i in {1..30}; do
    if mongosh --port 27017 --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        echo "   ✅ MongoDB prêt"
        break
    fi
    echo -n "."
    sleep 1
    
    if [ $i -eq 30 ]; then
        echo ""
        echo "❌ MongoDB ne répond pas après 30s"
        echo "Logs: tail -f $MONGO_DIR/db-1/mongod.log"
        exit 1
    fi
done

# 8. Initialisation du Replica Set
echo "⚙️  Initialisation du Replica Set..."
INIT_SCRIPT=$(cat <<'EOF'
try {
    // Vérifier si replSet est actif
    var replStatus = db.adminCommand({replSetGetStatus: 1});
    print("✅ Replica Set déjà configuré");
    
    // Réinitialiser si nécessaire
    print("Réinitialisation du Replica Set...");
    rs.reconfig({_id: "rs0", members: []}, {force: true});
    sleep(3000);
} catch (e) {
    // Pas encore configuré
    print("Configuration du nouveau Replica Set...");
}

// Initialiser le Replica Set
try {
    var config = {
        _id: "rs0",
        members: [
            { _id: 0, host: "localhost:27017", priority: 2 },
            { _id: 1, host: "localhost:27018", priority: 1 },
            { _id: 2, host: "localhost:27019", priority: 1 }
        ]
    };
    
    var result = rs.initiate(config);
    
    if (result.ok === 1) {
        print("✅ Replica Set initialisé avec succès");
    } else {
        print("❌ Erreur lors de l'initialisation: " + JSON.stringify(result));
        quit(1);
    }
} catch (error) {
    print("❌ Erreur: " + error.message);
    quit(1);
}

// Attendre l'élection
print("⏳ Attente de l'élection du Primary...");
for (var i = 0; i < 60; i++) {
    try {
        var status = rs.status();
        if (status.members && status.members.some(m => m.stateStr === "PRIMARY")) {
            var primary = status.members.find(m => m.stateStr === "PRIMARY");
            print("🎉 Primary élu: " + primary.name);
            break;
        }
    } catch (e) {}
    
    sleep(1000);
    
    if (i % 10 === 0) {
        print("... encore " + (60 - i) + " secondes");
    }
}
EOF
)

echo "$INIT_SCRIPT" | mongosh --port 27017 --quiet

# 9. Vérification finale
echo "🔍 Vérification finale..."
mongosh --port 27017 --quiet --eval "
try {
    var status = rs.status();
    print('✅ CONFIGURATION RÉUSSIE');
    print('');
    print('📊 STATUT DU REPLICA SET:');
    print('==========================');
    
    status.members.forEach(function(member, index) {
        var icon = '🔵';
        if (member.stateStr === 'PRIMARY') icon = '👑';
        if (member.stateStr === 'SECONDARY') icon = '🟢';
        if (member.stateStr === 'STARTUP' || member.stateStr === 'STARTUP2') icon = '🟡';
        
        var health = member.health === 1 ? '✅' : '❌';
        print(icon + ' ' + health + ' ' + member.name + ' - ' + member.stateStr);
    });
    
    print('');
    print('📈 Métriques:');
    print('   Primary: ' + (status.members.find(m => m.stateStr === 'PRIMARY')?.name || 'N/A'));
    print('   Secondaires: ' + status.members.filter(m => m.stateStr === 'SECONDARY').length);
    print('   OK: ' + status.ok);
    
} catch (e) {
    print('❌ Erreur de vérification: ' + e.message);
    print('');
    print('Conseils:');
    print('1. Vérifiez que MongoDB est bien installé');
    print('2. Vérifiez les logs: tail -f ' + '$MONGO_DIR' + '/db-*/mongod.log');
    print('3. Essayez de redémarrer manuellement');
}
"

echo ""
echo "========================================="
echo "✅ CONFIGURATION TERMINÉE !"
echo "========================================="
echo ""
echo "📋 Commandes utiles:"
echo "   mongosh --port 27017                 # Connexion au Primary"
echo "   rs.status()                          # Statut du Replica Set"
echo "   rs.conf()                            # Configuration"
echo ""
echo "📁 Répertoires de données:"
echo "   $MONGO_DIR/db-1"
echo "   $MONGO_DIR/db-2"
echo "   $MONGO_DIR/db-3"
echo ""
echo "📋 Logs:"
echo "   tail -f $MONGO_DIR/db-1/mongod.log"
echo ""
echo "🛑 Pour arrêter:"
echo "   ./scripts/phase3_replica/stop_replica.sh"
echo ""

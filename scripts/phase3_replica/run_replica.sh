#!/bin/bash
# run_replica.sh - Version corrigée

echo "=== REDÉMARRAGE MONGODB REPLICA SET ==="

# 1. Arrêter MongoDB proprement
echo "1. Arrêt des instances..."
# D'abord essayer d'arrêter proprement chaque instance
for port in 27017 27018 27019; do
    mongod --port $port --shutdown 2>/dev/null && echo "   Port $port arrêté proprement" || true
done

# 2. Attendre et kill forcé si nécessaire
sleep 3
echo "2. Nettoyage des processus restants..."
pkill -9 mongod 2>/dev/null || echo "   Aucun processus à killer"

# 3. Nettoyer les fichiers problématiques
echo "3. Nettoyage des fichiers temporaires..."
# Fichiers lock (très important)
find ./data/mongo -name "*.lock" -delete 2>/dev/null || true

# Sockets
rm -f /tmp/mongodb-*.sock 2>/dev/null || true
rm -f /tmp/.mongodb-*.sock 2>/dev/null || true

# 4. Vérifier/créer les répertoires
echo "4. Préparation des répertoires..."
for i in 1 2 3; do
    if [ ! -d "./data/mongo/db$i" ]; then
        mkdir -p "./data/mongo/db$i"
        echo "   Créé db$i"
    else
        echo "   db$i existe déjà"
    fi
done

# 5. Démarrer les instances
echo "5. Démarrage des instances..."
echo "   Port 27017..."
mongod --replSet rs0 --port 27017 --dbpath ./data/mongo/db-1 --bind_ip 127.0.0.1,localhost --fork --logpath ./data/mongo/db-1/mongod.log
sleep 2

echo "   Port 27018..."
mongod --replSet rs0 --port 27018 --dbpath ./data/mongo/db-2 --bind_ip 127.0.0.1,localhost --fork --logpath ./data/mongo/db-2/mongod.log
sleep 2

echo "   Port 27019..."
mongod --replSet rs0 --port 27019 --dbpath ./data/mongo/db-3 --bind_ip 127.0.0.1,localhost --fork --logpath ./data/mongo/db-3/mongod.log
sleep 2

# 6. Attendre que MongoDB soit vraiment prêt
echo "6. Attente démarrage complet..."
sleep 8

# 7. Vérification
echo "7. Vérification..."
if mongosh --port 27017 --quiet --eval "db.adminCommand('ping')" 2>/dev/null; then
    echo "=== ✅ SUCCÈS ==="
    echo "MongoDB Replica Set démarré sur:"
    echo "   Primary: localhost:27017"
    echo "   Secondaires: localhost:27018, localhost:27019"
    echo ""
    echo "Vérifier le statut: mongosh --port 27017 --eval 'rs.status()'"
else
    echo "=== ❌ ÉCHEC ==="
    echo "MongoDB ne répond pas. Vérifiez:"
    echo "1. Les logs: tail -f ./data/mongo/db-1/mongod.log"
    echo "2. Si les ports sont libres: netstat -tulpn | grep :2701"
    echo "3. Si MongoDB est installé: mongod --version"
    exit 1
fi
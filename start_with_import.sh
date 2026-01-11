#!/bin/bash
# startup.sh - Ultra simple

echo "Démarrage en cours..."

# 1. Setup replica
bash scripts/phase3_replica/setup_replica.sh

# 2. Migration
python3 scripts/phase2_mongodb/migrate_flat.py

# 3. Import
python3 scripts/phase3_replica/import_data.py

# 4. Django
python3 manage.py check
python3 manage.py runserver
#!/bin/bash
# startup.sh - Ultra simple

echo "Démarrage en cours..."

# 1. Setup replica
bash scripts/phase3_replica/run_replica.sh

# 4. Django
python3 manage.py check
python3 manage.py runserver
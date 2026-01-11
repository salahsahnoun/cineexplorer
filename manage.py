#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
import subprocess
from pathlib import Path

def check_and_start_services():
    """Vérifier et démarrer les services si nécessaire"""
    BASE_DIR = Path(__file__).resolve().parent
    
    # Vérifier si MongoDB est en cours d'exécution
    try:
        import pymongo
        client = pymongo.MongoClient('localhost:27017', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        print("✅ MongoDB déjà démarré")
        return True
    except:
        print("⚠️  MongoDB non détecté, tentative de démarrage...")
        
        # Essayer de démarrer via le script de setup
        setup_script = BASE_DIR / "scripts" / "phase3_replica" / "run_replica.sh"
        if setup_script.exists():
            try:
                subprocess.run([str(setup_script), "--import"], check=False)
                print("✅ Script de setup exécuté")
                return True
            except Exception as e:
                print(f"⚠️  Erreur démarrage: {e}")
                return False
        else:
            print("⚠️  Script de setup non trouvé")
            return False

def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    
    # Vérifier si c'est la commande runserver
    if len(sys.argv) > 1 and sys.argv[1] == 'runserver':
        print("\n🎬 CinéExplorer - Vérification des services...")
        check_and_start_services()
        print("\n" + "="*50)
    
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
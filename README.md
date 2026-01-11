Voici un modèle complet pour votre `README.md` :

```markdown
# 🎬 CinéExplorer - Plateforme Web de Découverte de Films

**Aix-Marseille Université – Polytech Marseille - Département Informatique**

---

## 📋 Description du Projet

CinéExplorer est une plateforme web complète permettant d'explorer une base de données de films (IMDB) avec une architecture évolutive intégrant SQLite, MongoDB et Django.

### 🎯 Objectifs pédagogiques
- Maîtriser les bases de données relationnelles (SQLite) et NoSQL (MongoDB)
- Implémenter une architecture multi-bases de données
- Configurer un Replica Set MongoDB pour la haute disponibilité
- Développer une application web professionnelle avec Django

---

## 🏗️ Architecture Technique

### Stack Technologique
- **Backend** : Django 4.x / Python 3.10+
- **Bases de données** :
  - SQLite 3 (Phase 1 - Données relationnelles)
  - MongoDB 6.x (Phase 2 & 3 - Données documents + Replica Set)
- **Frontend** : Bootstrap 5, Chart.js
- **Outils** : Git, Jupyter Notebook, pandas

### Architecture du Système
```
Application Django (Vues, Templates, Static)
        ↓
┌───────────────────────┐
│    Stratégie Multi-   │
│      Bases            │
└───────────────────────┘
        ↓
├── SQLite Service ──┤ Listes, Filtres, Requêtes complexes
└── MongoDB Service ─┘ Détails films, Documents structurés
        ↓
┌───────────────────────┐
│   MongoDB Replica Set │
│   • Primary: 27017    │
│   • Secondary: 27018  │
│   • Secondary: 27019  │
└───────────────────────┘
```

---

## 📂 Structure du Projet

```
cineexplorer/
├── config/                    # Configuration Django
├── movies/                    # Application principale
│   ├── models.py             # Modèles SQLite
│   ├── services/             # Services d'accès aux bases
│   │   ├── sqlite_service.py
│   │   └── mongo_service.py
│   └── templates/            # Templates HTML
├── data/                     # Données
│   ├── csv/                 # Fichiers IMDB originaux
│   ├── imdb.db              # Base SQLite générée
│   └── mongo/               # Données MongoDB
├── scripts/                  # Scripts par phase
│   ├── phase1_sqlite/       # Exploration et SQLite
│   ├── phase2_mongodb/      # Migration vers MongoDB
│   └── phase3_replica/      # Configuration Replica Set
├── reports/                  # Rapports PDF par livrable
├── exploration.ipynb        # Notebook d'analyse
├── manage.py                # Script de gestion Django
├── requirements.txt         # Dépendances Python
└── README.md                # Ce fichier
```

---

## 🚀 Installation et Configuration

### Prérequis
- Python 3.10+
- MongoDB 6.x
- Git

### 1. Cloner le dépôt
```bash
git clone <url-du-depot>
cd cineexplorer
```

### 2. Créer et activer l'environnement virtuel
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

### 3. Installer les dépendances
```bash
pip install -r requirements.txt
```

### 4. Importer les données (première utilisation)
```bash
# Option 1 : Script complet
./start_with_import.sh

# Option 2 : Manuellement
# a. Explorer les données
jupyter notebook data/exploration.ipynb

# b. Créer la base SQLite
python scripts/phase1_sqlite/create_schema.py
python scripts/phase1_sqlite/import_data.py
python scripts/phase1_sqlite/create_indexes.py

# c. Migrer vers MongoDB
python scripts/phase2_mongodb/migrate_flat.py
python scripts/phase2_mongodb/migrate_structured.py

# d. Configurer le Replica Set
./scripts/phase3_replica/setup_replica.sh
```

### 5. Démarrer l'application
```bash
# Si les données sont déjà importées
./startup.sh

#Sinon utilisé celui ci pour démarrer avec importation
./start_with_import.sh

# L'application sera accessible sur :
# http://localhost:8000
```

---

## 📊 Phases du Projet

### Phase 1 : Exploration et SQLite (25%)
- **T1.0** : Exploration des données IMDB (Jupyter Notebook)
- **T1.1** : Conception du schéma relationnel normalisé
- **T1.2** : Import des données dans SQLite
- **T1.3** : Requêtes SQL avancées (9 requêtes)
- **T1.4** : Indexation et benchmark de performance

### Phase 2 : Migration MongoDB (25%)
- **T2.1** : Installation et configuration MongoDB
- **T2.2** : Migration des collections plates
- **T2.3** : Requêtes MongoDB équivalentes
- **T2.4** : Documents structurés dénormalisés

### Phase 3 : Distribution et Replica Set (25%)
- **T3.1** : Configuration d'un Replica Set à 3 nœuds
- **T3.2** : Tests de tolérance aux pannes
- **T3.3** : Préparation de l'intégration Django

### Phase 4 : Interface Web Django (25%)
- **T4.1** : Pages web (Accueil, Liste, Détail, Recherche, Statistiques)
- **T4.2** : Stratégie d'intégration multi-bases
- **T4.3** : Design responsive avec Bootstrap 5

---

## 🌐 Pages de l'Application

### 1. Page d'Accueil (`/`)
- Statistiques générales (nombre de films, acteurs, etc.)
- Top 10 des films les mieux notés
- Formulaire de recherche rapide
- Films récemment ajoutés

### 2. Liste des Films (`/movies/`)
- Pagination (20 films par page)
- Filtres : genre, année, note minimale
- Tri par titre, année, note
- Affichage en grille ou liste

### 3. Détail d'un Film (`/movies/<id>/`)
- Informations complètes depuis MongoDB
- Casting avec personnages
- Réalisateurs et scénaristes
- Titres alternatifs par région
- Films similaires

### 4. Recherche (`/search/`)
- Recherche par titre de film
- Recherche par nom de personne
- Résultats groupés par type

### 5. Statistiques (`/stats/`)
- Films par genre (graphique en barres)
- Films par décennie (graphique linéaire)
- Distribution des notes (histogramme)
- Top 10 acteurs les plus prolifiques

---

## 🗃️ Stratégie Multi-Bases

| Fonctionnalité | Base utilisée | Justification |
|----------------|---------------|---------------|
| Liste films + filtres | SQLite | Requêtes relationnelles efficaces |
| Détail complet film | MongoDB | Document pré-agrégé, 1 seule requête |
| Statistiques agrégées | SQLite ou MongoDB | Selon la complexité |
| Recherche textuelle | SQLite (LIKE) | Simple et suffisant |

---

## 📁 Données IMDB

Le projet utilise un sous-ensemble des données IMDB :

- **imdb-small.zip** (recommandé) : ~10,000 films, ~50,000 personnes
- **imdb-tiny.zip** (tests rapides) : ~100 films, ~500 personnes
- **imdb-medium.zip** (performance) : ~100,000 films, ~500,000 personnes

Fichiers disponibles :
- `movies.csv` - Films (titre, année, durée)
- `persons.csv` - Personnes (acteurs, réalisateurs)
- `characters.csv` - Personnages joués
- `ratings.csv` - Notes et votes
- ... et 5 autres fichiers

---

## 📚 Commandes Utiles

### Gestion MongoDB
```bash
# Démarrer le Replica Set
./scripts/phase3_replica/setup_replica.sh

# Redémarrer MongoDB
./scripts/phase3_replica/run_replica.sh

### Développement
```bash
# Lancer le serveur de développement
python manage.py runserver

# Vérifier les erreurs
python manage.py check

# Ouvrir le shell Django
python manage.py shell
```

---

## 📄 Livrables

### Livrable 1 : Exploration et SQLite (25%)
- Code : Notebook + scripts Phase 1
- Rapport PDF (4-5 pages) : Exploration, schéma ER, requêtes, benchmark

### Livrable 2 : MongoDB (25%)
- Code : Scripts de migration et requêtes
- Rapport PDF (4-5 pages) : Modèle document, comparaison SQL/NoSQL

### Livrable 3 : Replica Set (25%)
- Code : Scripts de configuration et tests
- Rapport PDF (3-4 pages) : Architecture, tests de panne, analyse

### Livrable 4 : Projet Final (25%)
- Repository Git complet
- Application Django fonctionnelle
- Rapport final (8-10 pages) : Architecture, choix techniques, benchmarks

---

## 🔧 Dépannage

### Problèmes courants

1. **"Address already in use" (port 27017)**
   ```bash
   sudo lsof -i :27017
   sudo kill <PID>
   ```

2. **Module Django non trouvé**
   ```bash
   pip install django
   ```

3. **MongoDB ne démarre pas**
   ```bash
   # Vérifier les fichiers lock
   rm -f data/mongo/*/mongod.lock
   # Redémarrer
   ./scripts/phase3_replica/setup_replica.sh
   ```

4. **Erreur de connexion MongoDB dans Django**
   ```bash
   # Vérifier que MongoDB est en cours
   mongosh --eval "db.adminCommand('ping')"
   ```

### Logs à consulter
```bash
# Logs MongoDB
tail -f data/mongo/db-1/mongod.log

# Logs Django
tail -f logs/django.log  # si configuré
```

---

## 📖 Documentation

- [Documentation Django](https://docs.djangoproject.com/)
- [Documentation PyMongo](https://pymongo.readthedocs.io/)
- [Documentation MongoDB](https://docs.mongodb.com/)
- [Bootstrap 5](https://getbootstrap.com/docs/)
- [Chart.js](https://www.chartjs.org/docs/)

---

## 👥 Contribution

**Étudiant** : SAHNOUN Salah Eddine  
**Année** : 2025-2026

---

## 📄 Licence

Projet académique - Aix-Marseille Université - Polytech Marseille  
Utilisation strictement réservée à des fins pédagogiques.

---

*Dernière mise à jour : Janvier 2026*
```

Ce README est complet, professionnel et contient toutes les informations nécessaires pour comprendre, installer, utiliser et maintenir votre projet. Il suit les bonnes pratiques et est bien structuré pour un projet académique.

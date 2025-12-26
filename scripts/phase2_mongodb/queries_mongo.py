import sqlite3
import time
from pymongo import MongoClient
from pathlib import Path

class IMDBQueriesOptimized:
    def __init__(self, mongo_db_name='imdb_flat', sqlite_path='./data/imdb.db'):
        """
        Initialise les connexions aux bases de données et configure les index.
        """
        self.client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        self.db = self.client[mongo_db_name]
        
        # Vérifier si MongoDB est accessible
        try:
            self.client.admin.command('ping')
        except Exception as e:
            print(f"❌ MongoDB non accessible: {e}")
            print("Lancez MongoDB avec: mongod --dbpath ./data/mongo/standalone")
            raise
        
        # Connexion SQLite
        self.sqlite_conn = sqlite3.connect(sqlite_path)
        self.sqlite_conn.row_factory = sqlite3.Row
        
        print("🎬 PHASE 2 - REQUÊTES OPTIMISÉES MONGODB vs SQLITE")
        print("="*70)
        
        self._setup_indexes()
        print("✅ Connexions établies et index configurés")
    
    def _setup_indexes(self):
        """Configure les index optimaux pour MongoDB."""
        print("⚙️  Configuration des index MongoDB...")
        
        index_configs = {
            "persons": [
                [("pid", 1)], 
                [("primaryName", 1)],
                [("primaryName", "text")]
            ],
            "movies": [
                [("mid", 1)], 
                [("startYear", 1)],
                [("primaryTitle", 1)],
                [("startYear", 1), ("primaryTitle", 1)]
            ],
            "principals": [
                [("pid", 1)], 
                [("mid", 1)],
                [("pid", 1), ("mid", 1)],
                [("category", 1)],
                [("mid", 1), ("category", 1)]
            ],
            "ratings": [
                [("mid", 1)], 
                [("averageRating", -1)],
                [("numVotes", -1)],
                [("averageRating", -1), ("numVotes", -1)]
            ],
            "genres": [
                [("mid", 1)], 
                [("genre", 1)],
                [("genre", 1), ("mid", 1)]
            ],
            "directors": [
                [("mid", 1)], 
                [("pid", 1)],
                [("pid", 1), ("mid", 1)]
            ],
            "characters": [
                [("mid", 1)], 
                [("pid", 1)],
                [("mid", 1), ("pid", 1)],
                [("name", 1)]
            ],
            "writers": [
                [("mid", 1)], 
                [("pid", 1)]
            ]
        }
        
        total_indexes = 0
        for collection, indexes in index_configs.items():
            if collection in self.db.list_collection_names():
                for idx in indexes:
                    try:
                        self.db[collection].create_index(idx)
                        total_indexes += 1
                    except Exception as e:
                        print(f"  ⚠️  Erreur index {collection}: {e}")
        
        print(f"✅ {total_indexes} index créés/optimisés")
    
    def benchmark_query(self, q_id, description, mongo_pipeline, sql_query, params=()):
        """
        Exécute et compare une requête entre MongoDB et SQLite.
        """
        print(f"\n{'='*70}")
        print(f"🔍 {q_id} : {description}")
        print(f"{'='*70}")
        
        # MongoDB
        mongo_time = 0
        mongo_results_count = 0
        mongo_success = False
        
        try:
            start_m = time.perf_counter()
            collection = mongo_pipeline["collection"]
            pipeline = mongo_pipeline["pipeline"]
            
            cursor = self.db[collection].aggregate(
                pipeline, 
                allowDiskUse=True,
                maxTimeMS=30000
            )
            mongo_results = list(cursor)
            mongo_time = (time.perf_counter() - start_m) * 1000
            mongo_results_count = len(mongo_results)
            mongo_success = True
            
            print(f"   ✅ MongoDB : {mongo_time:>10.2f} ms | {mongo_results_count:>5} résultats")
            
        except Exception as e:
            mongo_time = 30000
            mongo_success = False
            error_msg = str(e)[:80]
            print(f"   ❌ MongoDB : TIMEOUT/ERREUR - {error_msg}")
        
        # SQLite
        sqlite_time = 0
        sqlite_results_count = 0
        
        try:
            start_s = time.perf_counter()
            cursor = self.sqlite_conn.cursor()
            
            if hasattr(self, '_last_query_time'):
                cursor.execute("PRAGMA cache_size = 0")
                cursor.execute("PRAGMA shrink_memory")
            
            cursor.execute(sql_query, params)
            sqlite_results = cursor.fetchall()
            sqlite_time = (time.perf_counter() - start_s) * 1000
            sqlite_results_count = len(sqlite_results)
            
            print(f"   ✅ SQLite  : {sqlite_time:>10.2f} ms | {sqlite_results_count:>5} résultats")
            
        except Exception as e:
            sqlite_time = 30000
            error_msg = str(e)[:80]
            print(f"   ❌ SQLite  : ERREUR - {error_msg}")
        
        # Analyse comparative
        if mongo_success and sqlite_time < 30000 and mongo_time > 0 and sqlite_time > 0:
            ratio = sqlite_time / mongo_time
            if ratio < 0.7:
                print(f"   🚀 MongoDB {1/ratio:.1f}x plus rapide")
            elif ratio > 1.3:
                print(f"   🚀 SQLite {ratio:.1f}x plus rapide")
            else:
                print(f"   ⚖️  Performances similaires (±30%)")
        
        # Vérification cohérence
        if mongo_success and mongo_results_count != sqlite_results_count:
            print(f"   ⚠️  Différence résultats: MongoDB={mongo_results_count}, SQLite={sqlite_results_count}")
        
        self._last_query_time = time.time()
        
        return {
            "query_id": q_id,
            "description": description,
            "mongo_time": mongo_time,
            "sqlite_time": sqlite_time,
            "mongo_success": mongo_success,
            "mongo_results": mongo_results_count,
            "sqlite_results": sqlite_results_count
        }
    
    def run_all_queries(self):
        """Exécute les 9 requêtes optimisées."""
        results = []
        
        # Q1: Filmographie d'un acteur
        results.append(self.benchmark_query(
            "Q1", "Filmographie de Tom Hanks",
            {
                "collection": "principals",
                "pipeline": [
                    {"$match": {"category": {"$in": ["actor", "actress"]}}},
                    {"$lookup": {
                        "from": "persons",
                        "localField": "pid",
                        "foreignField": "pid",
                        "as": "person"
                    }},
                    {"$unwind": "$person"},
                    {"$match": {"person.primaryName": "Tom Hanks"}},
                    {"$lookup": {
                        "from": "movies",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "movie"
                    }},
                    {"$unwind": "$movie"},
                    {"$lookup": {
                        "from": "ratings",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "rating"
                    }},
                    {"$unwind": {"path": "$rating", "preserveNullAndEmptyArrays": True}},
                    {"$project": {
                        "title": "$movie.primaryTitle",
                        "year": "$movie.startYear",
                        "character": "$job",
                        "rating": {"$ifNull": ["$rating.averageRating", None]}
                    }},
                    {"$sort": {"year": -1}},
                    {"$limit": 20}
                ]
            },
            """
            SELECT m.primaryTitle, m.startYear, pr.job, r.averageRating
            FROM persons p
            JOIN principals pr ON p.pid = pr.pid
            JOIN movies m ON pr.mid = m.mid
            LEFT JOIN ratings r ON m.mid = r.mid
            WHERE p.primaryName = ?
            AND pr.category IN ('actor', 'actress')
            ORDER BY m.startYear DESC
            LIMIT 20
            """,
            ("Tom Hanks",)
        ))
        
        # Q2: Top N films par genre sur période
        results.append(self.benchmark_query(
            "Q2", "Top 5 films Drama 1990-2000",
            {
                "collection": "movies",
                "pipeline": [
                    {"$match": {
                        "startYear": {"$gte": 1990, "$lte": 2000},
                        "titleType": "movie"
                    }},
                    {"$lookup": {
                        "from": "genres",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "genre_info"
                    }},
                    {"$unwind": "$genre_info"},
                    {"$match": {"genre_info.genre": "Drama"}},
                    {"$lookup": {
                        "from": "ratings",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "rating"
                    }},
                    {"$unwind": "$rating"},
                    {"$match": {"rating.numVotes": {"$gt": 1000}}},
                    {"$project": {
                        "title": "$primaryTitle",
                        "year": "$startYear",
                        "rating": "$rating.averageRating",
                        "votes": "$rating.numVotes"
                    }},
                    {"$sort": {"rating": -1, "votes": -1}},
                    {"$limit": 5}
                ]
            },
            """
            SELECT m.primaryTitle, m.startYear, r.averageRating, r.numVotes
            FROM movies m
            JOIN genres g ON m.mid = g.mid
            JOIN ratings r ON m.mid = r.mid
            WHERE g.genre = ?
            AND m.startYear BETWEEN ? AND ?
            AND m.titleType = 'movie'
            AND r.numVotes > 1000
            ORDER BY r.averageRating DESC, r.numVotes DESC
            LIMIT 5
            """,
            ("Drama", 1990, 2000)
        ))
        
        # Q3: Acteurs multi-rôles
        results.append(self.benchmark_query(
            "Q3", "Acteurs avec plusieurs rôles dans un film",
            {
                "collection": "characters",
                "pipeline": [
                    {"$group": {
                        "_id": {"movie": "$mid", "actor": "$pid"},
                        "role_count": {"$sum": 1},
                        "characters": {"$push": "$name"}
                    }},
                    {"$match": {"role_count": {"$gt": 1}}},
                    {"$lookup": {
                        "from": "persons",
                        "localField": "_id.actor",
                        "foreignField": "pid",
                        "as": "actor_info"
                    }},
                    {"$lookup": {
                        "from": "movies",
                        "localField": "_id.movie",
                        "foreignField": "mid",
                        "as": "movie_info"
                    }},
                    {"$unwind": "$actor_info"},
                    {"$unwind": "$movie_info"},
                    {"$project": {
                        "actor": "$actor_info.primaryName",
                        "movie": "$movie_info.primaryTitle",
                        "year": "$movie_info.startYear",
                        "multiple_roles": "$role_count",
                        "characters": {"$slice": ["$characters", 3]}
                    }},
                    {"$sort": {"multiple_roles": -1, "actor": 1}},
                    {"$limit": 10}
                ]
            },
            """
            SELECT p.primaryName, m.primaryTitle, m.startYear, COUNT(*) as role_count
            FROM characters c
            JOIN persons p ON c.pid = p.pid
            JOIN movies m ON c.mid = m.mid
            GROUP BY c.pid, c.mid
            HAVING COUNT(*) > 1
            ORDER BY role_count DESC, p.primaryName
            LIMIT 10
            """
        ))
        
        # Q4: Collaborations réalisateurs-acteurs
        results.append(self.benchmark_query(
            "Q4", "Collaborations réalisateurs-acteurs",
            {
                "collection": "principals",
                "pipeline": [
                    {"$match": {"category": {"$in": ["actor", "actress"]}}},
                    {"$lookup": {
                        "from": "directors",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "directors"
                    }},
                    {"$unwind": "$directors"},
                    {"$group": {
                        "_id": {
                            "actor_pid": "$pid",
                            "director_pid": "$directors.pid"
                        },
                        "collaboration_count": {"$sum": 1}
                    }},
                    {"$match": {"collaboration_count": {"$gte": 3}}},
                    {"$lookup": {
                        "from": "persons",
                        "localField": "_id.actor_pid",
                        "foreignField": "pid",
                        "as": "actor_info"
                    }},
                    {"$lookup": {
                        "from": "persons",
                        "localField": "_id.director_pid",
                        "foreignField": "pid",
                        "as": "director_info"
                    }},
                    {"$unwind": "$actor_info"},
                    {"$unwind": "$director_info"},
                    {"$project": {
                        "actor": "$actor_info.primaryName",
                        "director": "$director_info.primaryName",
                        "collaboration_count": 1
                    }},
                    {"$sort": {"collaboration_count": -1}},
                    {"$limit": 10}
                ]
            },
            """
            SELECT 
                pa.primaryName as actor,
                pd.primaryName as director,
                COUNT(*) as collaboration_count
            FROM principals pr
            JOIN directors d ON pr.mid = d.mid
            JOIN persons pa ON pr.pid = pa.pid
            JOIN persons pd ON d.pid = pd.pid
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY pr.pid, d.pid
            HAVING COUNT(*) >= 3
            ORDER BY collaboration_count DESC
            LIMIT 10
            """
        ))
        
        # Q5: Genres populaires
        results.append(self.benchmark_query(
            "Q5", "Genres populaires (note > 7.0, > 50 films, votes > 10k)",
            {
                "collection": "ratings",
                "pipeline": [
                    {"$match": {"numVotes": {"$gt": 10000}}},
                    {"$lookup": {
                        "from": "genres",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "genres"
                    }},
                    {"$unwind": "$genres"},
                    {"$group": {
                        "_id": "$genres.genre",
                        "avg_rating": {"$avg": "$averageRating"},
                        "movie_count": {"$sum": 1}
                    }},
                    {"$match": {
                        "avg_rating": {"$gt": 7.0},
                        "movie_count": {"$gt": 50}
                    }},
                    {"$project": {
                        "genre": "$_id",
                        "average_rating": {"$round": ["$avg_rating", 2]},
                        "movie_count": 1
                    }},
                    {"$sort": {"average_rating": -1, "movie_count": -1}}
                ]
            },
            """
            SELECT 
                g.genre,
                ROUND(AVG(r.averageRating), 2) as average_rating,
                COUNT(DISTINCT g.mid) as movie_count
            FROM ratings r
            JOIN genres g ON r.mid = g.mid
            WHERE r.numVotes > 10000
            GROUP BY g.genre
            HAVING AVG(r.averageRating) > 7.0
               AND COUNT(DISTINCT g.mid) > 50
            ORDER BY average_rating DESC, movie_count DESC
            """
        ))
        
        # Q6: Évolution carrière par décennie
        results.append(self.benchmark_query(
            "Q6", "Évolution carrière Tom Hanks par décennie",
            {
                "collection": "persons",
                "pipeline": [
                    {"$match": {"primaryName": "Tom Hanks"}},
                    {"$lookup": {
                        "from": "principals",
                        "localField": "pid",
                        "foreignField": "pid",
                        "as": "career"
                    }},
                    {"$unwind": "$career"},
                    {"$match": {"career.category": {"$in": ["actor", "actress"]}}},
                    {"$lookup": {
                        "from": "movies",
                        "localField": "career.mid",
                        "foreignField": "mid",
                        "as": "movie"
                    }},
                    {"$unwind": "$movie"},
                    {"$match": {"movie.titleType": "movie", "movie.startYear": {"$ne": None}}},
                    {"$lookup": {
                        "from": "ratings",
                        "localField": "movie.mid",
                        "foreignField": "mid",
                        "as": "rating"
                    }},
                    {"$unwind": {"path": "$rating", "preserveNullAndEmptyArrays": True}},
                    {"$addFields": {
                        "decade": {
                            "$subtract": [
                                "$movie.startYear",
                                {"$mod": ["$movie.startYear", 10]}
                            ]
                        },
                        "movie_rating": {"$ifNull": ["$rating.averageRating", 0]}
                    }},
                    {"$group": {
                        "_id": "$decade",
                        "film_count": {"$sum": 1},
                        "avg_rating": {"$avg": "$movie_rating"}
                    }},
                    {"$sort": {"_id": 1}},
                    {"$project": {
                        "decade": "$_id",
                        "film_count": 1,
                        "average_rating": {"$round": ["$avg_rating", 2]}
                    }}
                ]
            },
            """
            SELECT 
                (m.startYear - (m.startYear % 10)) as decade,
                COUNT(*) as film_count,
                ROUND(AVG(COALESCE(r.averageRating, 0)), 2) as average_rating
            FROM persons p
            JOIN principals pr ON p.pid = pr.pid
            JOIN movies m ON pr.mid = m.mid
            LEFT JOIN ratings r ON m.mid = r.mid
            WHERE p.primaryName = ?
            AND pr.category IN ('actor', 'actress')
            AND m.titleType = 'movie'
            AND m.startYear IS NOT NULL
            GROUP BY decade
            ORDER BY decade
            """,
            ("Tom Hanks",)
        ))
        
        # Q7: Classement top 3 par genre
        results.append(self.benchmark_query(
            "Q7", "Top 3 films par genre",
            {
                "collection": "genres",
                "pipeline": [
                    {"$lookup": {
                        "from": "movies",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "movie"
                    }},
                    {"$unwind": "$movie"},
                    {"$match": {"movie.titleType": "movie"}},
                    {"$lookup": {
                        "from": "ratings",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "rating"
                    }},
                    {"$unwind": "$rating"},
                    {"$match": {"rating.numVotes": {"$gt": 10000}}},
                    {"$addFields": {
                        "rating_val": "$rating.averageRating",
                        "votes": "$rating.numVotes"
                    }},
                    {"$sort": {"genre": 1, "rating_val": -1, "votes": -1}},
                    {"$group": {
                        "_id": "$genre",
                        "films": {
                            "$push": {
                                "title": "$movie.primaryTitle",
                                "year": "$movie.startYear",
                                "rating": "$rating_val",
                                "votes": "$votes"
                            }
                        }
                    }},
                    {"$project": {
                        "genre": "$_id",
                        "top_films": {"$slice": ["$films", 3]}
                    }},
                    {"$unwind": "$top_films"},
                    {"$project": {
                        "genre": 1,
                        "title": "$top_films.title",
                        "year": "$top_films.year",
                        "rating": "$top_films.rating",
                        "rank": {"$add": [{"$indexOfArray": ["$films", "$top_films"]}, 1]}
                    }},
                    {"$sort": {"genre": 1, "rating": -1}},
                    {"$limit": 15}
                ]
            },
            """
            WITH ranked_movies AS (
                SELECT 
                    g.genre,
                    m.primaryTitle,
                    m.startYear,
                    r.averageRating,
                    r.numVotes,
                    ROW_NUMBER() OVER (
                        PARTITION BY g.genre 
                        ORDER BY r.averageRating DESC, r.numVotes DESC
                    ) as rank
                FROM genres g
                JOIN movies m ON g.mid = m.mid
                JOIN ratings r ON m.mid = r.mid
                WHERE r.numVotes > 10000
                  AND m.titleType = 'movie'
            )
            SELECT genre, primaryTitle, startYear, averageRating, rank
            FROM ranked_movies
            WHERE rank <= 3
            ORDER BY genre, rank
            LIMIT 15
            """
        ))
        
        # Q8: Percée grâce à un film
        results.append(self.benchmark_query(
            "Q8", "Personnes ayant percé grâce à un film (>200k votes)",
            {
                "collection": "ratings",
                "pipeline": [
                    {"$match": {"numVotes": {"$gt": 200000}}},
                    {"$lookup": {
                        "from": "principals",
                        "localField": "mid",
                        "foreignField": "mid",
                        "as": "contributors"
                    }},
                    {"$unwind": "$contributors"},
                    {"$lookup": {
                        "from": "persons",
                        "localField": "contributors.pid",
                        "foreignField": "pid",
                        "as": "person_info"
                    }},
                    {"$unwind": "$person_info"},
                    {"$group": {
                        "_id": "$person_info.pid",
                        "name": {"$first": "$person_info.primaryName"},
                        "breakthrough_films": {"$addToSet": "$mid"},
                        "total_votes": {"$sum": "$numVotes"}
                    }},
                    {"$project": {
                        "person": "$name",
                        "breakthrough_count": {"$size": "$breakthrough_films"},
                        "total_votes": 1
                    }},
                    {"$sort": {"breakthrough_count": -1, "total_votes": -1}},
                    {"$limit": 10}
                ]
            },
            """
            SELECT 
                p.primaryName,
                COUNT(DISTINCT r.mid) as breakthrough_count,
                SUM(r.numVotes) as total_votes
            FROM ratings r
            JOIN principals pr ON r.mid = pr.mid
            JOIN persons p ON pr.pid = p.pid
            WHERE r.numVotes > 200000
            GROUP BY p.pid, p.primaryName
            ORDER BY breakthrough_count DESC, total_votes DESC
            LIMIT 10
            """
        ))
        
        # Q9: Acteur-Réalisateur
        results.append(self.benchmark_query(
            "Q9", "Personnes à la fois acteur et réalisateur",
            {
                "collection": "persons",
                "pipeline": [
                    {"$lookup": {
                        "from": "principals",
                        "localField": "pid",
                        "foreignField": "pid",
                        "as": "acting_roles"
                    }},
                    {"$match": {"acting_roles.category": {"$in": ["actor", "actress"]}}},
                    {"$lookup": {
                        "from": "directors",
                        "localField": "pid",
                        "foreignField": "pid",
                        "as": "directing_roles"
                    }},
                    {"$match": {"directing_roles.0": {"$exists": True}}},
                    {"$project": {
                        "name": "$primaryName",
                        "acting_count": {"$size": "$acting_roles"},
                        "directing_count": {"$size": "$directing_roles"}
                    }},
                    {"$sort": {"acting_count": -1, "directing_count": -1}},
                    {"$limit": 10}
                ]
            },
            """
            SELECT 
                p.primaryName,
                COUNT(DISTINCT pr.mid) as acting_count,
                COUNT(DISTINCT d.mid) as directing_count
            FROM persons p
            LEFT JOIN principals pr ON p.pid = pr.pid AND pr.category IN ('actor', 'actress')
            LEFT JOIN directors d ON p.pid = d.pid
            GROUP BY p.pid, p.primaryName
            HAVING COUNT(DISTINCT pr.mid) > 0 AND COUNT(DISTINCT d.mid) > 0
            ORDER BY acting_count DESC, directing_count DESC
            LIMIT 10
            """
        ))
        
        return results
    
    def print_summary(self, results):
        """Affiche un résumé des résultats."""
        print(f"\n{'='*70}")
        print("📊 RÉSUMÉ DES RÉSULTATS")
        print(f"{'='*70}")
        
        successful_mongo = [r for r in results if r["mongo_success"]]
        mongo_times = [r["mongo_time"] for r in successful_mongo]
        sqlite_times = [r["sqlite_time"] for r in results]
        
        avg_mongo = sum(mongo_times) / len(mongo_times) if mongo_times else 0
        avg_sqlite = sum(sqlite_times) / len(sqlite_times) if sqlite_times else 0
        
        print(f"Requêtes MongoDB réussies: {len(successful_mongo)}/9")
        print(f"Temps moyen MongoDB: {avg_mongo/1000:.3f} secondes")
        print(f"Temps moyen SQLite: {avg_sqlite/1000:.3f} secondes")
        
        if avg_mongo > 0:
            ratio = avg_sqlite / avg_mongo
            print(f"Rapport SQLite/MongoDB: {ratio:.2f}")
            if ratio > 1:
                print(f"→ SQLite {ratio:.1f}x plus rapide en moyenne")
            else:
                print(f"→ MongoDB {1/ratio:.1f}x plus rapide en moyenne")
        
        # Comptage des victoires
        mongo_wins = 0
        sqlite_wins = 0
        ties = 0
        
        for res in results:
            if res["mongo_success"] and res["mongo_time"] > 0 and res["sqlite_time"] > 0:
                ratio = res["sqlite_time"] / res["mongo_time"]
                if ratio < 0.7:
                    mongo_wins += 1
                elif ratio > 1.3:
                    sqlite_wins += 1
                else:
                    ties += 1
        
        print(f"\n🏆 VICTOIRES PAR SYSTÈME:")
        print(f"  MongoDB: {mongo_wins} requêtes")
        print(f"  SQLite: {sqlite_wins} requêtes")
        print(f"  Égalités: {ties} requêtes")
    
    def close_connections(self):
        """Ferme proprement les connexions aux bases de données."""
        if hasattr(self, 'client'):
            self.client.close()
        if hasattr(self, 'sqlite_conn'):
            self.sqlite_conn.close()
        print("\n🔌 Connexions fermées")


def main():
    """Fonction principale exécutant le benchmark complet."""
    print("🎬 LANCEMENT DU BENCHMARK COMPLET ET OPTIMISÉ")
    print("="*70)
    
    try:
        # Initialisation
        benchmark = IMDBQueriesOptimized()
        
        # Exécution des requêtes
        print("\n⚡ Exécution des 9 requêtes optimisées...")
        results = benchmark.run_all_queries()
        
        # Résumé
        benchmark.print_summary(results)
        
        print(f"\n{'='*70}")
        print("✅ BENCHMARK TERMINÉ AVEC SUCCÈS !")
        print(f"{'='*70}")
        
        # Fermeture propre
        benchmark.close_connections()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrompu par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur critique: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
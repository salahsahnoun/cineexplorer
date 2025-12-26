#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "imdb_flat"
OUTPUT_COLL = "movies_complete"

BENCH_MOVIE_ID = "tt0111161"
ALLOW_DISK_USE = True

# Mets une valeur (ex: 20000) pour tester vite, sinon None pour full.
MIGRATION_LIMIT = None

class T24MoviesComplete:
    def __init__(self):
        self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self.client.admin.command("ping")
        self.db = self.client[DB_NAME]
        print(f"🔌 Connecté à MongoDB / DB={DB_NAME}")

    def create_source_indexes(self):
        print("\n=== ÉTAPE 1 : INDEX SOURCES ===")

        indexes = [
            ("movies", [("mid", ASCENDING)]),
            ("movies", [("titleType", ASCENDING)]),
            ("movies", [("startYear", ASCENDING)]),

            ("ratings", [("mid", ASCENDING)]),

            ("genres", [("mid", ASCENDING)]),
            ("genres", [("genre", ASCENDING), ("mid", ASCENDING)]),

            ("directors", [("mid", ASCENDING)]),
            ("directors", [("pid", ASCENDING)]),
            ("directors", [("mid", ASCENDING), ("pid", ASCENDING)]),

            ("principals", [("mid", ASCENDING)]),
            ("principals", [("pid", ASCENDING)]),
            ("principals", [("mid", ASCENDING), ("category", ASCENDING)]),
            ("principals", [("mid", ASCENDING), ("ordering", ASCENDING)]),

            ("persons", [("pid", ASCENDING)]),

            # Pour récupérer les personnages vite (mid,pid)
            ("characters", [("mid", ASCENDING), ("pid", ASCENDING)]),

            ("writers", [("mid", ASCENDING)]),
            ("writers", [("pid", ASCENDING)]),
            ("writers", [("mid", ASCENDING), ("pid", ASCENDING)]),

            ("titles", [("mid", ASCENDING)]),
            ("titles", [("region", ASCENDING), ("mid", ASCENDING)]),
        ]

        for col, spec in indexes:
            t0 = time.time()
            self.db[col].create_index(spec)
            print(f"   ✅ {col} {spec} ({time.time()-t0:.2f}s)")

    def build_movies_complete(self, limit=MIGRATION_LIMIT):
        print(f"\n=== ÉTAPE 2 : BUILD {OUTPUT_COLL} ===")
        if limit:
            print(f"⚠️  MODE TEST limit={limit}")
        else:
            print("🚀 MODE COMPLET")

        pipeline = []

        # (Optionnel mais souvent attendu) : uniquement les films
        pipeline.append({"$match": {"titleType": "movie"}})

        if limit:
            pipeline.append({"$limit": int(limit)})

        # Ratings (0..1)
        pipeline += [
            {"$lookup": {"from": "ratings", "localField": "mid", "foreignField": "mid", "as": "r"}},
            {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        ]

        # Genres (N -> ["Drama", ...])
        pipeline += [
            {"$lookup": {"from": "genres", "localField": "mid", "foreignField": "mid", "as": "g"}},
            {"$addFields": {"genres": {"$setUnion": ["$g.genre", []]}}},
        ]

        # Directors (N -> [{person_id,name}, ...])
        pipeline += [
            {"$lookup": {
                "from": "directors",
                "let": {"mid": "$mid"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$mid", "$$mid"]}}},
                    {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
                    {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
                    {"$project": {"_id": 0, "person_id": "$pid", "name": "$p.primaryName"}},
                    {"$sort": {"name": 1}}
                ],
                "as": "directors"
            }},
        ]

        # Cast : principals(actor/actress) + persons + characters + ordering
        # characters: table characters(mid,pid,name)
        pipeline += [
            {"$lookup": {
                "from": "principals",
                "let": {"mid": "$mid"},
                "pipeline": [
                    {"$match": {
                        "$expr": {"$eq": ["$mid", "$$mid"]},
                        "category": {"$in": ["actor", "actress"]}
                    }},
                    {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
                    {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},

                    # Récupérer les personnages (peut être plusieurs)
                    {"$lookup": {
                        "from": "characters",
                        "let": {"pid": "$pid", "mid": "$mid"},
                        "pipeline": [
                            {"$match": {"$expr": {"$and": [
                                {"$eq": ["$pid", "$$pid"]},
                                {"$eq": ["$mid", "$$mid"]}
                            ]}}},
                            {"$project": {"_id": 0, "name": 1}}
                        ],
                        "as": "chars"
                    }},

                    # Transformer chars -> ["Andy Dufresne", ...]
                    {"$addFields": {
                        "characters": {"$map": {"input": "$chars", "as": "c", "in": "$$c.name"}}
                    }},

                    {"$project": {
                        "_id": 0,
                        "person_id": "$pid",
                        "name": "$p.primaryName",
                        "characters": 1,
                        "ordering": "$ordering"
                    }},
                    {"$sort": {"ordering": 1}}
                ],
                "as": "cast"
            }},
        ]

        # Writers (N -> [{person_id,name,category}, ...])
        # ⚠️ Si ta table writers n'a PAS de category, on met category=null
        pipeline += [
            {"$lookup": {
                "from": "writers",
                "let": {"mid": "$mid"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$mid", "$$mid"]}}},
                    {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
                    {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
                    {"$project": {
                        "_id": 0,
                        "person_id": "$pid",
                        "name": "$p.primaryName",
                        # adapte si tu as un champ catégorie dans writers (sinon None)
                        "category": {"$ifNull": ["$category", None]}
                    }},
                    {"$sort": {"name": 1}}
                ],
                "as": "writers"
            }},
        ]

        # Titles (N -> [{region,title}, ...])
        pipeline += [
            {"$lookup": {
                "from": "titles",
                "let": {"mid": "$mid"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$mid", "$$mid"]}}},
                    {"$project": {"_id": 0, "region": 1, "title": 1}},
                    {"$sort": {"region": 1}}
                ],
                "as": "titles"
            }},
        ]

        # Projection finale EXACTE (structure énoncé)
        pipeline += [
            {"$project": {
                "_id": "$mid",
                "title": "$primaryTitle",
                "year": "$startYear",
                "runtime": "$runtimeMinutes",
                "genres": "$genres",
                "rating": {
                    "average": "$r.averageRating",
                    "votes": "$r.numVotes"
                },
                "directors": "$directors",
                "cast": "$cast",
                "writers": "$writers",
                "titles": "$titles"
            }},
            {"$merge": {"into": OUTPUT_COLL, "whenMatched": "replace", "whenNotMatched": "insert"}}
        ]

        t0 = time.time()
        self.db.movies.aggregate(pipeline, allowDiskUse=ALLOW_DISK_USE)
        dt = time.time() - t0
        count = self.db[OUTPUT_COLL].count_documents({})
        print(f"✅ movies_complete construit en {dt:.2f}s — {count:,} documents")

    def index_target(self):
        print("\n=== ÉTAPE 3 : INDEX TARGET ===")
        c = self.db[OUTPUT_COLL]
        c.create_index([("title", ASCENDING)])
        c.create_index([("year", ASCENDING)])
        c.create_index([("genres", ASCENDING)])
        c.create_index([("rating.average", DESCENDING), ("rating.votes", DESCENDING)])
        c.create_index([("directors.person_id", ASCENDING)])
        c.create_index([("cast.person_id", ASCENDING)])
        c.create_index([("writers.person_id", ASCENDING)])
        c.create_index([("titles.region", ASCENDING)])
        print("✅ Index target OK")

    def benchmark(self, movie_id=BENCH_MOVIE_ID):
        print("\n=== ÉTAPE 4 : COMPARAISONS (temps / stockage / complexité) ===")
        print(f"🎯 Film: {movie_id}")

        # 1 requête (structuré)
        t0 = time.perf_counter()
        structured = self.db[OUTPUT_COLL].find_one({"_id": movie_id})
        t_struct = (time.perf_counter() - t0) * 1000

        # N requêtes (flat) : reconstruire un doc équivalent
        t0 = time.perf_counter()
        movie = self.db.movies.find_one({"mid": movie_id})
        rating = self.db.ratings.find_one({"mid": movie_id})
        genres = list(self.db.genres.find({"mid": movie_id}, {"_id": 0, "genre": 1}))
        directors = list(self.db.directors.aggregate([
            {"$match": {"mid": movie_id}},
            {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
            {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
            {"$project": {"_id": 0, "person_id": "$pid", "name": "$p.primaryName"}},
        ], allowDiskUse=True))

        cast = list(self.db.principals.aggregate([
            {"$match": {"mid": movie_id, "category": {"$in": ["actor", "actress"]}}},
            {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
            {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
            {"$lookup": {
                "from": "characters",
                "let": {"pid": "$pid", "mid": "$mid"},
                "pipeline": [
                    {"$match": {"$expr": {"$and": [
                        {"$eq": ["$pid", "$$pid"]},
                        {"$eq": ["$mid", "$$mid"]}
                    ]}}},
                    {"$project": {"_id": 0, "name": 1}}
                ],
                "as": "chars"
            }},
            {"$addFields": {"characters": {"$map": {"input": "$chars", "as": "c", "in": "$$c.name"}}}},
            {"$project": {"_id": 0, "person_id": "$pid", "name": "$p.primaryName", "characters": 1, "ordering": "$ordering"}},
            {"$sort": {"ordering": 1}}
        ], allowDiskUse=True))

        writers = list(self.db.writers.aggregate([
            {"$match": {"mid": movie_id}},
            {"$lookup": {"from": "persons", "localField": "pid", "foreignField": "pid", "as": "p"}},
            {"$unwind": {"path": "$p", "preserveNullAndEmptyArrays": True}},
            {"$project": {"_id": 0, "person_id": "$pid", "name": "$p.primaryName", "category": {"$ifNull": ["$category", None]}}},
        ], allowDiskUse=True))

        titles = list(self.db.titles.find({"mid": movie_id}, {"_id": 0, "region": 1, "title": 1}))

        flat_doc = {
            "_id": movie_id,
            "title": movie.get("primaryTitle") if movie else None,
            "year": movie.get("startYear") if movie else None,
            "runtime": movie.get("runtimeMinutes") if movie else None,
            "genres": [g["genre"] for g in genres],
            "rating": {"average": rating.get("averageRating") if rating else None, "votes": rating.get("numVotes") if rating else None},
            "directors": directors,
            "cast": cast,
            "writers": writers,
            "titles": titles
        }

        t_flat = (time.perf_counter() - t0) * 1000

        print("\n⏱ Temps récupération film complet")
        print(f"   Structuré (1 requête) : {t_struct:.3f} ms")
        print(f"   Flat (N requêtes)     : {t_flat:.3f} ms")
        if t_struct > 0:
            print(f"   Gain (flat/struct)    : x{t_flat/t_struct:.2f}")

        # Stockage
        def coll_mb(name):
            return self.db.command("collstats", name)["storageSize"] / (1024**2)

        flat_cols = ["movies", "ratings", "genres", "directors", "principals", "persons", "writers", "titles", "characters"]
        flat_size = sum(coll_mb(c) for c in flat_cols if c in self.db.list_collection_names())
        struct_size = coll_mb(OUTPUT_COLL)

        print("\n💾 Taille de stockage")
        print(f"   Flat (somme collections) : {flat_size:.2f} MB")
        print(f"   Structuré (movies_complete): {struct_size:.2f} MB")

        # Complexité code (qualitative, demandé par l’énoncé)
        print("\n🧩 Complexité du code (qualitatif)")
        print("   • Flat: plusieurs requêtes + logique de reconstruction côté appli (plus long, plus fragile).")
        print("   • Structuré: 1 requête simple côté appli (plus simple à utiliser), mais migration/pipeline plus complexe et duplication.")

        # Petit check
        if structured:
            print("\n✅ Exemple champs movies_complete:")
            print("   keys:", list(structured.keys()))
        else:
            print("\n⚠️ film non trouvé dans movies_complete (filtre titleType=movie ? ID ?).")

    def close(self):
        self.client.close()


def main():
    try:
        app = T24MoviesComplete()
        app.create_source_indexes()
        app.build_movies_complete(limit=MIGRATION_LIMIT)
        app.index_target()
        app.benchmark(movie_id=BENCH_MOVIE_ID)
        app.close()
        print("\n✅ T2.4 terminé et conforme à l’énoncé.")
    except ServerSelectionTimeoutError:
        print("❌ MongoDB non accessible (mongod/service non démarré).")
    except Exception as e:
        print(f"❌ Erreur: {e}")
        raise


if __name__ == "__main__":
    main()

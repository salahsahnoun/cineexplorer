import time
from pathlib import Path
from scripts.phase1_sqlite.queries import (
    get_connection,
    query_actor_filmography,
    query_top_movies_by_genre_period,
    query_multi_role_actors,
    query_actor_director_collaborations,
    query_popular_genres,
    query_actor_career_by_decade,
    query_top3_by_genre,
    query_boosted_careers,
    query_top_directors,
)

RESULTS_FILE = Path("data") / "benchmark_t1_4.txt"


def time_call(fn, *args, repeat=3):
    """
    Mesure le temps moyen d'exécution d'une fonction SQL (en ms).
    On exécute la fonction `repeat` fois et on prend la moyenne.
    """
    durations = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        _ = fn(*args)
        t1 = time.perf_counter()
        durations.append((t1 - t0) * 1000.0)  # ms
    return sum(durations) / len(durations)


def main():
    conn = get_connection()

    # ⚠️ Choisir des paramètres "réalistes" pour les requêtes
    actor_example = "Smith"      # un nom courant, plus de chances d'exister
    genre_example = "Drama"
    start_year = 1990
    end_year = 2010

    results = []

    print("=== Benchmark T1.4 (sans index dédiés) ===\n")

    # Q1
    t_q1 = time_call(query_actor_filmography, conn, actor_example)
    print(f"Q1 - Filmographie acteur : {t_q1:.2f} ms")
    results.append(("Q1 - Filmographie", t_q1))

    # Q2
    t_q2 = time_call(
        query_top_movies_by_genre_period,
        conn,
        genre_example,
        start_year,
        end_year,
        50,
    )
    print(f"Q2 - Top N films par genre/période : {t_q2:.2f} ms")
    results.append(("Q2 - Top N films", t_q2))

    # Q3
    t_q3 = time_call(query_multi_role_actors, conn)
    print(f"Q3 - Acteurs multi-rôles : {t_q3:.2f} ms")
    results.append(("Q3 - Multi-rôles", t_q3))

    # Q4
    t_q4 = time_call(query_actor_director_collaborations, conn, actor_example)
    print(f"Q4 - Collaborations acteur/réalisateurs : {t_q4:.2f} ms")
    results.append(("Q4 - Collabs acteur-réalisateurs", t_q4))

    # Q5
    t_q5 = time_call(query_popular_genres, conn)
    print(f"Q5 - Genres populaires : {t_q5:.2f} ms")
    results.append(("Q5 - Genres populaires", t_q5))

    # Q6
    t_q6 = time_call(query_actor_career_by_decade, conn, actor_example)
    print(f"Q6 - Carrière par décennie : {t_q6:.2f} ms")
    results.append(("Q6 - Carrière acteur", t_q6))

    # Q7
    t_q7 = time_call(query_top3_by_genre, conn)
    print(f"Q7 - Top 3 par genre : {t_q7:.2f} ms")
    results.append(("Q7 - Top 3 genre", t_q7))

    # Q8
    t_q8 = time_call(query_boosted_careers, conn)
    print(f"Q8 - Carrières propulsées : {t_q8:.2f} ms")
    results.append(("Q8 - Boosted careers", t_q8))

    # Q9
    t_q9 = time_call(query_top_directors, conn)
    print(f"Q9 - Top réalisateurs : {t_q9:.2f} ms")
    results.append(("Q9 - Top réalisateurs", t_q9))

    conn.close()

    # Sauvegarde des résultats en texte pour le rapport
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_FILE.open("w", encoding="utf-8") as f:
        f.write("Requête;Sans index (ms)\n")
        for name, t in results:
            f.write(f"{name};{t:.2f}\n")

    print(f"\n✅ Résultats enregistrés dans {RESULTS_FILE}")


if __name__ == "__main__":
    main()

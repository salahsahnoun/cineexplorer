import sqlite3
import time
from pathlib import Path
from textwrap import dedent
import csv

DB_PATH = Path("data") / "imdb.db"
RESULTS_CSV = Path("data") / "benchmark_t1_4_corrected.csv"
N_RUNS = 3  # nombre de répétitions par requête

# --- Définition des requêtes T1.3 (Q1 à Q9) ---
QUERIES = {
    "Q1": {
        "label": "Filmographie acteur",
        "sql": dedent("""
            SELECT
                m.primaryTitle,
                m.startYear,
                COALESCE(c.name, '') AS characterName,
                r.averageRating
            FROM persons p
            JOIN principals pr ON pr.pid = p.pid
            JOIN movies m ON m.mid = pr.mid
            LEFT JOIN characters c
                ON c.mid = pr.mid AND c.pid = pr.pid
            LEFT JOIN ratings r
                ON r.mid = m.mid
            WHERE p.primaryName LIKE '%Hanks%'
              AND pr.category IN ('actor', 'actress')
            ORDER BY m.startYear, m.primaryTitle;
        """).strip()
    },
    "Q2": {
        "label": "Top N films par genre/période",
        "sql": dedent("""
            SELECT
                m.primaryTitle,
                m.startYear,
                r.averageRating
            FROM movies m
            JOIN ratings r ON r.mid = m.mid
            JOIN genres g ON g.mid = m.mid
            WHERE g.genre = 'Drama'
              AND m.startYear BETWEEN 1990 AND 2000
            ORDER BY r.averageRating DESC
            LIMIT 10;
        """).strip()
    },
    "Q3": {
        "label": "Acteurs multi-rôles",
        "sql": dedent("""
            SELECT
                p.primaryName,
                m.primaryTitle,
                COUNT(*) AS rolesCount
            FROM characters c
            JOIN persons p ON p.pid = c.pid
            JOIN movies m ON m.mid = c.mid
            GROUP BY p.pid, m.mid
            HAVING COUNT(*) > 1
            ORDER BY rolesCount DESC;
        """).strip()
    },
    "Q4": {
        "label": "Collaborations acteur/réalisateurs",
        "sql": dedent("""
            SELECT
                a.primaryName AS actor,
                d.primaryName AS director,
                COUNT(*) AS collaborations
            FROM principals pr
            JOIN persons a ON a.pid = pr.pid
            JOIN directors dir ON dir.mid = pr.mid
            JOIN persons d ON d.pid = dir.pid
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY a.pid, d.pid
            HAVING collaborations >= 3
            ORDER BY collaborations DESC;
        """).strip()
    },
    "Q5": {
        "label": "Genres populaires",
        "sql": dedent("""
            SELECT
                g.genre,
                AVG(r.averageRating) AS avgRating,
                COUNT(*) AS filmCount
            FROM movies m
            JOIN genres g ON g.mid = m.mid
            JOIN ratings r ON r.mid = m.mid
            GROUP BY g.genre
            ORDER BY avgRating DESC;
        """).strip()
    },
    "Q6": {
        "label": "Carrière par décennie",
        "sql": dedent("""
            SELECT
                p.primaryName,
                (m.startYear / 10) * 10 AS decade,
                COUNT(*) AS filmCount
            FROM persons p
            JOIN principals pr ON pr.pid = p.pid
            JOIN movies m ON m.mid = pr.mid
            WHERE p.primaryName LIKE '%Hanks%'
            GROUP BY p.primaryName, decade
            ORDER BY decade;
        """).strip()
    },
    "Q7": {
        "label": "Top 3 par genre (window)",
        "sql": dedent("""
            SELECT genre, primaryTitle, averageRating
            FROM (
                SELECT
                    g.genre,
                    m.primaryTitle,
                    r.averageRating,
                    ROW_NUMBER() OVER (
                        PARTITION BY g.genre
                        ORDER BY r.averageRating DESC
                    ) AS rn
                FROM movies m
                JOIN ratings r ON r.mid = m.mid
                JOIN genres g ON g.mid = m.mid
            )
            WHERE rn <= 3;
        """).strip()
    },
    "Q8": {
        "label": "Carrières propulsées",
        "sql": dedent("""
            SELECT
                p.primaryName,
                m.primaryTitle AS debutFilm,
                r.averageRating
            FROM persons p
            JOIN principals pr ON pr.pid = p.pid
            JOIN movies m ON m.mid = pr.mid
            JOIN ratings r ON r.mid = m.mid
            WHERE pr.ordering = 1
              AND r.averageRating >= 8.0
            ORDER BY r.averageRating DESC;
        """).strip()
    },
    "Q9": {
        "label": "Top réalisateurs",
        "sql": dedent("""
            SELECT
                d.primaryName,
                COUNT(*) AS filmCount,
                AVG(r.averageRating) AS avgRating
            FROM directors dir
            JOIN persons d ON dir.pid = d.pid
            JOIN ratings r ON r.mid = dir.mid
            GROUP BY d.pid
            HAVING COUNT(*) >= 5
            ORDER BY avgRating DESC;
        """).strip()
    },
}

# --- Index OPTIMISÉS (basés sur l'analyse des plans) ---
OPTIMAL_INDEXES = """
    -- === INDEX CRITIQUES (optimisation majeure) ===
    
    -- 1. Pour Q1, Q4, Q6, Q8 : Index composite sur principals (JOIN fréquent)
    CREATE INDEX IF NOT EXISTS idx_principals_pid_mid_category 
        ON principals(pid, mid, category);
    
    -- 2. Pour Q2, Q7 : Index sur genres (filtre WHERE genre='...')
    CREATE INDEX IF NOT EXISTS idx_genres_mid_genre 
        ON genres(mid, genre);
    
    -- 3. Pour Q3 : Index sur characters (GROUP BY pid, mid)
    CREATE INDEX IF NOT EXISTS idx_characters_pid_mid 
        ON characters(pid, mid);
    
    -- 4. Pour Q4, Q9 : Index sur directors
    CREATE INDEX IF NOT EXISTS idx_directors_mid_pid 
        ON directors(mid, pid);
    
    -- 5. Pour Q2, Q5, Q7, Q8, Q9 : Index sur ratings
    CREATE INDEX IF NOT EXISTS idx_ratings_mid_rating 
        ON ratings(mid, averageRating);
    
    -- 6. Pour Q2, Q6 : Index sur movies.startYear (filtre BETWEEN)
    CREATE INDEX IF NOT EXISTS idx_movies_startYear_mid 
        ON movies(startYear, mid);
    
    -- 7. Pour Q8 : Index spécifique pour ordering=1
    CREATE INDEX IF NOT EXISTS idx_principals_ordering_pid_mid 
        ON principals(ordering, pid, mid);
    
    -- 8. Pour requêtes avec HAVING COUNT(*): index sur writers
    CREATE INDEX IF NOT EXISTS idx_writers_mid_pid 
        ON writers(mid, pid);
    
    -- === INDEX FACULTATIFS (gain marginal) ===
    
    -- 9. Pour Q5 : Index couvrant pour AVG() et COUNT()
    CREATE INDEX IF NOT EXISTS idx_genres_rating_cov 
        ON genres(genre, mid);
    
    -- 10. Index pour les recherches textuelles (LIKE) - limité
    -- NOTE: LIKE '%...%' ne bénéficie pas d'index B-tree standard
    -- Mais peut aider pour les recherches exactes
    CREATE INDEX IF NOT EXISTS idx_persons_primaryName 
        ON persons(primaryName);
"""

# --- Fonctions utilitaires ---

def connect_db() -> sqlite3.Connection:
    """Établit une connexion à la base SQLite avec optimisations."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")  # Meilleures performances
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def drop_custom_indexes(conn: sqlite3.Connection):
    """Supprime uniquement les index créés manuellement."""
    print("\n🗑️  Suppression des index existants...")
    
    # Liste des index à supprimer (uniquement ceux qu'on va recréer)
    custom_indexes = [
        'idx_principals_pid_mid_category',
        'idx_genres_mid_genre',
        'idx_characters_pid_mid',
        'idx_directors_mid_pid',
        'idx_ratings_mid_rating',
        'idx_movies_startYear_mid',
        'idx_principals_ordering_pid_mid',
        'idx_writers_mid_pid',
        'idx_genres_rating_cov',
        'idx_persons_primaryName'
    ]
    
    dropped = 0
    for idx in custom_indexes:
        try:
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
            dropped += 1
        except Exception as e:
            print(f"  ⚠️  Impossible de supprimer {idx}: {e}")
    
    conn.commit()
    print(f"  ✅ {dropped} index supprimés")
    return dropped


def get_existing_indexes(conn: sqlite3.Connection) -> list:
    """Récupère la liste des index existants."""
    cur = conn.cursor()
    cur.execute("""
        SELECT name, sql 
        FROM sqlite_master 
        WHERE type = 'index' 
        AND name NOT LIKE 'sqlite_autoindex%'
    """)
    return cur.fetchall()


def measure_query_time(conn: sqlite3.Connection, sql: str, n_runs: int = N_RUNS) -> float:
    """
    Exécute la requête n_runs fois et renvoie le temps moyen en millisecondes.
    Avec vidage du cache entre chaque exécution.
    """
    cur = conn.cursor()
    total = 0.0
    
    for run in range(n_runs):
        # Vider le cache SQLite avant chaque exécution
        if run > 0:
            conn.execute("PRAGMA cache_size = 0")
            conn.execute("PRAGMA shrink_memory")
        
        t0 = time.perf_counter()
        cur.execute(sql).fetchall()
        t1 = time.perf_counter()
        total += (t1 - t0)
        
        # Afficher progression
        if (run + 1) % 3 == 0:
            print(f"    Run {run + 1}/{n_runs}: {(t1 - t0) * 1000:.1f} ms")
    
    return (total / n_runs) * 1000.0  # ms


def explain_query_plan(conn: sqlite3.Connection, name: str, sql: str, phase: str = ""):
    """
    Analyse et affiche le plan d'exécution d'une requête.
    """
    phase_label = f" ({phase})" if phase else ""
    print(f"\n--- EXPLAIN QUERY PLAN {phase_label} pour {name} ---")
    
    cur = conn.cursor()
    try:
        cur.execute("EXPLAIN QUERY PLAN " + sql)
        rows = cur.fetchall()
        
        if not rows:
            print("  (Aucun plan disponible)")
            return []
        
        for row in rows:
            # row = (id, parent, notused, detail)
            detail = row[3]
            
            # Ajouter des emojis pour la lisibilité
            if "SCAN TABLE" in detail:
                detail = "🔍 " + detail + " (Table Scan - Coûteux)"
            elif "SEARCH TABLE" in detail and "USING INDEX" in detail:
                detail = "✅ " + detail + " (Index utilisé)"
            elif "USING COVERING INDEX" in detail:
                detail = "🚀 " + detail + " (Covering Index - Optimal)"
            elif "USE TEMP B-TREE" in detail:
                detail = "⚠️  " + detail + " (Tri temporaire)"
            
            print(f"  {detail}")
        
        return rows
        
    except Exception as e:
        print(f"  ❌ Erreur EXPLAIN: {e}")
        return []


def format_size(bytes_size: int) -> str:
    """Convertit une taille en octets en format lisible."""
    for unit in ["o", "Ko", "Mo", "Go"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} To"


def analyze_plan_improvement(plan_before: list, plan_after: list, query_name: str) -> dict:
    """
    Analyse l'amélioration du plan d'exécution.
    """
    analysis = {
        "query": query_name,
        "scan_to_search": False,
        "temp_tree_removed": False,
        "covering_index": False,
        "improvement_score": 0
    }
    
    before_str = str(plan_before)
    after_str = str(plan_after)
    
    # Vérifier les améliorations
    if "SCAN TABLE" in before_str and "SEARCH TABLE" in after_str:
        analysis["scan_to_search"] = True
        analysis["improvement_score"] += 30
    
    if "USE TEMP B-TREE" in before_str and "USE TEMP B-TREE" not in after_str:
        analysis["temp_tree_removed"] = True
        analysis["improvement_score"] += 25
    
    if "USING COVERING INDEX" in after_str:
        analysis["covering_index"] = True
        analysis["improvement_score"] += 20
    
    # Points bonus pour optimisation
    if "sqlite_autoindex" not in after_str:
        analysis["improvement_score"] += 15
    
    return analysis


# --- Main corrigée ---

def main():
    """Benchmark corrigé avec analyse approfondie."""
    print("="*70)
    print("🚀 BENCHMARK CORRIGÉ - Phase 1.4 : Indexation et Performance")
    print("="*70)
    
    if not DB_PATH.exists():
        print(f"❌ Base de données non trouvée : {DB_PATH}")
        print("   Exécutez d'abord create_schema.py et import_data.py")
        return
    
    # Taille initiale
    size_before = DB_PATH.stat().st_size
    print(f"\n📊 ANALYSE INITIALE")
    print(f"  Taille base: {format_size(size_before)}")
    
    conn = connect_db()
    
    # === ÉTAPE 1: État initial (avec auto-index seulement) ===
    print("\n" + "="*70)
    print("📈 ÉTAPE 1 : Mesure avec AUTO-INDEX (état initial)")
    print("="*70)
    
    # Lister les index existants
    existing_indexes = get_existing_indexes(conn)
    print(f"\nIndex existants ({len(existing_indexes)}):")
    for idx_name, idx_sql in existing_indexes[:5]:  # Afficher les 5 premiers
        print(f"  • {idx_name}")
    if len(existing_indexes) > 5:
        print(f"  • ... et {len(existing_indexes)-5} autres")
    
    times_before = {}
    plans_before = {}
    
    print("\n⏱️  Mesure des performances...")
    for qname, meta in QUERIES.items():
        print(f"\n{qname} - {meta['label']}:")
        t_ms = measure_query_time(conn, meta["sql"])
        times_before[qname] = t_ms
        print(f"  Temps moyen: {t_ms:.2f} ms")
        
        # Analyser le plan initial
        plan = explain_query_plan(conn, qname, meta["sql"], "AVANT indexation")
        plans_before[qname] = plan
    
    # === ÉTAPE 2: Création des index optimisés ===
    print("\n" + "="*70)
    print("🔧 ÉTAPE 2 : Création d'index OPTIMISÉS")
    print("="*70)
    
    # Supprimer d'abord les anciens index
    drop_custom_indexes(conn)
    
    # Créer les nouveaux index
    print("\nCréation des index optimisés...")
    conn.executescript(OPTIMAL_INDEXES)
    conn.commit()
    
    # Vérifier la création
    new_indexes = get_existing_indexes(conn)
    print(f"\n✅ {len(new_indexes)} index créés:")
    for idx_name, idx_sql in new_indexes:
        # Extraire la partie pertinente du SQL
        if idx_sql:
            idx_info = idx_sql.split('ON ')[1][:50] + "..."
            print(f"  • {idx_name} → {idx_info}")
    
    # Taille après indexation
    conn.close()  # Fermer pour forcer l'écriture sur disque
    size_after = DB_PATH.stat().st_size
    size_increase = size_after - size_before
    size_increase_pct = (size_increase / size_before) * 100
    
    print(f"\n📦 Impact sur la taille:")
    print(f"  Avant: {format_size(size_before)}")
    print(f"  Après: {format_size(size_after)}")
    print(f"  Augmentation: {format_size(size_increase)} (+{size_increase_pct:.1f}%)")
    
    # === ÉTAPE 3: Mesure avec les nouveaux index ===
    print("\n" + "="*70)
    print("📈 ÉTAPE 3 : Mesure avec INDEX OPTIMISÉS")
    print("="*70)
    
    conn = connect_db()
    times_after = {}
    plans_after = {}
    plan_analyses = {}
    
    print("\n⏱️  Mesure des performances...")
    for qname, meta in QUERIES.items():
        print(f"\n{qname} - {meta['label']}:")
        t_ms = measure_query_time(conn, meta["sql"])
        times_after[qname] = t_ms
        print(f"  Temps moyen: {t_ms:.2f} ms")
        
        # Analyser le plan après indexation
        plan = explain_query_plan(conn, qname, meta["sql"], "APRÈS indexation")
        plans_after[qname] = plan
        
        # Comparer les plans
        if qname in plans_before:
            analysis = analyze_plan_improvement(
                plans_before[qname], 
                plan, 
                qname
            )
            plan_analyses[qname] = analysis
    
    conn.close()
    
    # === ÉTAPE 4: Analyse des résultats ===
    print("\n" + "="*70)
    print("📊 ÉTAPE 4 : ANALYSE DES RÉSULTATS")
    print("="*70)
    
    # Tableau principal
    print("\n" + "="*90)
    print("📋 TABLEAU SYNTHÈSE DES PERFORMANCES")
    print("="*90)
    header = f"{'Req':<4} | {'Description':<30} | {'Avant (ms)':>12} | {'Après (ms)':>12} | {'Gain':>10} | {'Score':>6} | {'Optimisation'}"
    print(header)
    print("-" * len(header))
    
    total_gain_pct = 0
    queries_with_gain = 0
    results_data = []
    
    for qname, meta in QUERIES.items():
        before = times_before[qname]
        after = times_after.get(qname, before)
        
        # Calcul gain
        if before > 0:
            gain_pct = ((before - after) / before) * 100
        else:
            gain_pct = 0
        
        total_gain_pct += gain_pct
        
        # Score d'amélioration
        score = plan_analyses.get(qname, {}).get("improvement_score", 0)
        
        # Type d'optimisation
        analysis = plan_analyses.get(qname, {})
        optimizations = []
        if analysis.get("scan_to_search"):
            optimizations.append("SCAN→SEARCH")
        if analysis.get("temp_tree_removed"):
            optimizations.append("No TEMP")
        if analysis.get("covering_index"):
            optimizations.append("COVERING")
        
        opt_str = ", ".join(optimizations) if optimizations else "limité"
        
        # Couleur pour le gain
        gain_str = f"{gain_pct:+.1f}%"
        if gain_pct > 10:
            gain_str = f"✅ {gain_pct:+.1f}%"
        elif gain_pct < -5:
            gain_str = f"❌ {gain_pct:+.1f}%"
        elif gain_pct > 0:
            gain_str = f"✓ {gain_pct:+.1f}%"
        
        if gain_pct > 0:
            queries_with_gain += 1
        
        print(f"{qname:<4} | {meta['label']:<30} | {before:>12.2f} | {after:>12.2f} | {gain_str:>10} | {score:>6} | {opt_str}")
        
        # Préparer données pour CSV
        results_data.append({
            "requete": qname,
            "description": meta["label"],
            "temps_avant_ms": before,
            "temps_apres_ms": after,
            "gain_pct": gain_pct,
            "score_optimisation": score,
            "optimisations": opt_str
        })
    
    # Statistiques globales
    avg_gain = total_gain_pct / len(QUERIES)
    avg_time_before = sum(times_before.values()) / len(times_before)
    avg_time_after = sum(times_after.values()) / len(times_after)
    
    print("\n" + "="*90)
    print("📈 STATISTIQUES GLOBALES")
    print("="*90)
    print(f"Gain moyen: {avg_gain:+.1f}%")
    print(f"Temps moyen avant: {avg_time_before:.2f} ms")
    print(f"Temps moyen après: {avg_time_after:.2f} ms")
    print(f"Requêtes améliorées: {queries_with_gain}/{len(QUERIES)}")
    print(f"Augmentation taille: {format_size(size_increase)} (+{size_increase_pct:.1f}%)")
    
    # Analyse coût-bénéfice
    if size_increase > 0 and avg_gain > 0:
        cost_benefit = avg_gain / size_increase_pct
        print(f"Ratio coût/bénéfice: {cost_benefit:.2f}% gain par % d'augmentation")
        
        if cost_benefit > 1.5:
            print(" CONCLUSION: Indexation TRÈS bénéfique")
        elif cost_benefit > 0.5:
            print("✓ CONCLUSION: Indexation bénéfique")
        else:
            print("⚠️  CONCLUSION: Indexation à optimiser")
    
    # === ÉTAPE 5: Sauvegarde des résultats ===
    print("\n" + "="*70)
    print(" ÉTAPE 5 : SAUVEGARDE DES RÉSULTATS")
    print("="*70)
    
    # Sauvegarde CSV
    try:
        with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ["requete", "description", "temps_avant_ms", "temps_apres_ms", 
                         "gain_pct", "score_optimisation", "optimisations"]
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(results_data)
        
        print(f"✅ Résultats sauvegardés: {RESULTS_CSV}")
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde CSV: {e}")
    
    # Génération d'un rapport texte
    report_path = Path("data") / "benchmark_report.txt"
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("RAPPORT DE BENCHMARK - Phase 1.4\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Date: {time.ctime()}\n")
            f.write(f"Base de données: {DB_PATH}\n")
            f.write(f"Taille initiale: {format_size(size_before)}\n")
            f.write(f"Taille finale: {format_size(size_after)}\n")
            f.write(f"Augmentation: {format_size(size_increase)} (+{size_increase_pct:.1f}%)\n\n")
            
            f.write("RÉSULTATS DÉTAILLÉS:\n")
            f.write("-"*70 + "\n")
            for qname, meta in QUERIES.items():
                before = times_before[qname]
                after = times_after.get(qname, before)
                gain = ((before - after) / before * 100) if before > 0 else 0
                f.write(f"{qname}: {meta['label']}\n")
                f.write(f"  Avant: {before:.2f} ms | Après: {after:.2f} ms | Gain: {gain:+.1f}%\n\n")
            
            f.write("STATISTIQUES:\n")
            f.write(f"- Gain moyen: {avg_gain:+.1f}%\n")
            f.write(f"- Requêtes améliorées: {queries_with_gain}/{len(QUERIES)}\n")
            f.write(f"- Temps moyen avant: {avg_time_before:.2f} ms\n")
            f.write(f"- Temps moyen après: {avg_time_after:.2f} ms\n")
        
        print(f"✅ Rapport généré: {report_path}")
        
    except Exception as e:
        print(f"⚠️  Erreur génération rapport: {e}")
    
    print("\n" + "="*70)
    print(" BENCHMARK TERMINÉ AVEC SUCCÈS")
    print("="*70)


if __name__ == "__main__":
    main()
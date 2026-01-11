"""
Vues Django pour T3.3 et Phase 4
"""
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.core.paginator import Paginator
from django.db.models import Q
import time
from django.template.defaulttags import register
import random

from .services import sqlite_service, mongo_service, home_service

# Créer des filtres template personnalisés
@register.filter
def intcomma(value):
    """Formatte un nombre avec des virgules"""
    try:
        return f"{int(value):,}".replace(",", " ")
    except:
        return value

@register.filter
def intword(value):
    """Formatte les grands nombres (1.2k, 3.4M)"""
    try:
        value = int(value)
        if value >= 1000000:
            return f"{value/1000000:.1f}M"
        elif value >= 1000:
            return f"{value/1000:.1f}k"
        return str(value)
    except:
        return value

@register.filter
def multiply(value, arg):
    """Multiplie deux nombres"""
    try:
        return float(value) * float(arg)
    except:
        return 0

@register.filter
def get_item(dictionary, key):
    """Récupère un élément d'un dictionnaire dans un template"""
    return dictionary.get(key)

def test_view(request):
    """
    Vue de test principale pour T3.3
    Affiche les connexions aux bases de données
    """
    # Récupérer les statistiques
    sqlite_stats = sqlite_service.get_movie_stats()
    mongo_stats = mongo_service.get_mongo_stats()
    
    # Déterminer l'état MongoDB
    if mongo_stats.get('replica_status') == 'ok':
        mongo_status = '✅ OK'
        mongo_error = None
    else:
        mongo_status = '❌ Erreur'
        mongo_error = mongo_stats.get('error', 'Erreur inconnue')
    
    # Préparer le contexte
    context = {
        'sqlite_stats': sqlite_stats,
        'mongo_stats': mongo_stats,
        'connections': {
            'sqlite': '✅ OK' if 'error' not in sqlite_stats else '❌ Erreur',
            'mongo': mongo_status
        },
        'mongo_error': mongo_error,
        'phase': 'T3.3 - Préparation Django'
    }
    
    return render(request, 'movies/test.html', context)

def api_test(request):
    """
    API JSON pour les tests T3.3
    """
    sqlite_stats = sqlite_service.get_movie_stats()
    mongo_stats = mongo_service.get_mongo_stats()
    
    response_data = {
        'status': 'success',
        'phase': 'T3.3 Django Integration',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'databases': {
            'sqlite': sqlite_stats,
            'mongodb': mongo_stats
        }
    }
    
    return JsonResponse(response_data)

def home_view(request):
    """
    Page d'accueil simple pour T3.3
    """
    return render(request, 'movies/home.html', {
        'title': 'CinéExplorer - T3.3',
        'phase': 'Phase 3: Distribution et Replica Set',
        'task': 'T3.3 - Préparation Django'
    })

def home_view_phase4(request):
    """
    Page d'accueil pour Phase 4
    """
    stats = home_service.get_home_stats()
    top_movies = stats.get('top_movies', [])
    
    # Ajouter width_percentage à chaque film pour la barre de progression
    for movie in top_movies:
        rating = movie.get('rating', 0)
        movie['width_percentage'] = (rating / 10) * 100
    
    context = {
        'stats': stats,
        'top_movies': top_movies,
        'random_movies': stats.get('random_movies', []),
        'title': 'CinéExplorer - Découvrez des films'
    }
    
    return render(request, 'movies/home.html', context)

def search_view(request):
    """Page de recherche T4.1.4 avec résultats groupés"""
    query = request.GET.get('q', '').strip()
    search_results = {}
    movies_count = 0
    persons_count = 0
    
    if query:
        # Recherche combinée
        search_results = home_service.search_all(query, limit_per_type=15)
        
        movies_count = search_results['total_movies']
        persons_count = search_results['total_persons']
        
        # Pour la compatibilité avec l'ancien template
        results = search_results['all']
    else:
        results = []
        search_results = {
            'movies': [],
            'persons': [],
            'all': [],
            'total_movies': 0,
            'total_persons': 0,
            'total': 0
        }
    
    # Calculer le total d'éléments dans la base (estimation)
    stats = sqlite_service.get_movie_stats()
    total_items = stats.get('total_movies', 0) + stats.get('total_persons', 0)
    
    context = {
        'query': query,
        'results': results,  # Pour l'ancien template
        'search_results': search_results,  # Nouvelle structure
        'movies': search_results['movies'],
        'persons': search_results['persons'],
        'movies_count': movies_count,
        'persons_count': persons_count,
        'total_items': total_items,
        'title': f'Recherche: {query}' if query else 'Recherche'
    }
    
    return render(request, 'movies/search.html', context)

def movie_list_view(request):
    """Liste des films avec pagination et filtres"""
    # Récupérer les paramètres GET
    genre = request.GET.get('genre', '')
    year_from = request.GET.get('year_from', '')
    year_to = request.GET.get('year_to', '')
    min_rating = request.GET.get('min_rating', '')
    sort = request.GET.get('sort', '-rating')
    page_number = request.GET.get('page', 1)
    
    # Récupérer les films filtrés
    movies_data = sqlite_service.get_filtered_movies(
        genre=genre,
        year_from=year_from,
        year_to=year_to,
        min_rating=min_rating,
        sort=sort
    )
    
    # Pagination
    paginator = Paginator(movies_data, 20)
    page_obj = paginator.get_page(page_number)
    
    # Récupérer les genres pour le filtre
    genres = sqlite_service.get_all_genres()
    
    # Statistiques
    stats = sqlite_service.get_movie_stats()
    
    context = {
        'movies': page_obj,
        'genres': genres,
        'selected_genre': genre,
        'year_from': year_from,
        'year_to': year_to,
        'min_rating': min_rating,
        'sort': sort,
        'total_movies': len(movies_data),
        'avg_rating': stats.get('avg_rating', 0),
        'latest_year': stats.get('latest_year', '2024'),
        'title': 'Liste des films'
    }
    
    return render(request, 'movies/list.html', context)

def movie_detail_view(request, movie_id):
    """Détail d'un film avec casting complet"""
    movie = None
    source = None
    
    print(f"\n=== CHARGEMENT FILM {movie_id} ===")
    
    # 1. Essayer MongoDB d'abord
    try:
        movie = mongo_service.get_complete_movie_with_characters(movie_id)
        if movie and movie.get('cast'):
            source = 'MongoDB'
            print(f"✓ Film trouvé dans MongoDB avec {len(movie['cast'])} acteurs")
        elif movie:
            source = 'MongoDB'
            print(f"⚠ Film trouvé dans MongoDB mais sans casting ({len(movie.get('cast', []))} acteurs)")
        else:
            print(f"✗ Film non trouvé dans MongoDB")
    except Exception as e:
        print(f"Erreur MongoDB: {e}")
    
    # 2. Si pas dans MongoDB ou sans casting, essayer SQLite
    if not movie or not movie.get('cast'):
        movie = sqlite_service.get_movie_with_characters(movie_id)
        if movie and movie.get('cast'):
            source = 'SQLite'
            print(f"✓ Film trouvé dans SQLite avec {len(movie['cast'])} acteurs")
        elif movie:
            source = 'SQLite' 
            print(f"⚠ Film trouvé dans SQLite mais sans casting")
        else:
            print(f"✗ Film non trouvé dans SQLite")
    
    # 3. Si toujours pas trouvé, retourner erreur
    if not movie:
        from django.http import Http404
        raise Http404(f"Film avec l'ID {movie_id} non trouvé")
    
    # 4. Assurer que tous les champs existent
    movie.setdefault('cast', [])
    movie.setdefault('directors', [])
    movie.setdefault('writers', [])
    movie.setdefault('titles', [])
    movie.setdefault('genres', [])
    movie.setdefault('rating', None)
    movie.setdefault('votes', None)
    movie.setdefault('runtime', None)
    movie.setdefault('language', 'en')
    movie.setdefault('isAdult', False)
    movie.setdefault('description', '')
    
    # 5. Statistiques de casting
    casting_stats = {
        'total_actors': len(movie['cast']),
        'actors_with_characters': sum(1 for actor in movie['cast'] if actor.get('characters')),
        'total_characters': sum(len(actor.get('characters', [])) for actor in movie['cast'])
    }
    
    print(f"📊 Statistiques casting: {casting_stats}")
    
    # 6. Films similaires
    similar_movies = sqlite_service.get_similar_movies_sqlite(
        movie_id,
        genres=movie.get('genres', []),
        directors=movie.get('directors', []),
        limit=4
    )
    
    # 7. Préparer le contexte
    context = {
        'movie': movie,
        'movie_id': movie_id,
        'similar_movies': similar_movies,
        'source': source,
        'casting_stats': casting_stats,
        'title': f"{movie.get('title', 'Détail du film')} - CinéExplorer"
    }
    
    return render(request, 'movies/detail.html', context)

def stats_view(request):
    """Page statistiques avec graphiques"""
    # Récupérer les statistiques depuis SQLite
    stats = sqlite_service.get_extended_stats()
    
    # Préparer les données pour les graphiques
    genres_distribution = stats.get('genres_distribution', [])
    for genre in genres_distribution:
        if stats.get('total_movies', 0) > 0:
            genre['percentage'] = (genre['count'] / stats['total_movies']) * 100
        else:
            genre['percentage'] = 0
    
    # Distribution des notes (exemple)
    rating_distribution = [100, 200, 500, 800, 1200, 1500, 1800, 1200, 800, 300]
    
    # Décennies (exemple)
    decades = [
        {'decade': '1950s', 'count': 500},
        {'decade': '1960s', 'count': 800},
        {'decade': '1970s', 'count': 1200},
        {'decade': '1980s', 'count': 1800},
        {'decade': '1990s', 'count': 2500},
        {'decade': '2000s', 'count': 3000},
        {'decade': '2010s', 'count': 3500},
        {'decade': '2020s', 'count': 2000},
    ]
    
    # Top films
    top_movies = home_service.get_top_rated_movies(limit=10)
    
    # Top acteurs
    top_actors = sqlite_service.get_top_actors(limit=10)
    
    context = {
        'total_movies': stats.get('total_movies', 0),
        'total_persons': stats.get('total_persons', 0),
        'avg_rating': stats.get('avg_rating', 0),
        'years_range': f"{stats.get('earliest_year', 1900)}-{stats.get('latest_year', 2024)}",
        
        # Données pour graphiques
        'genres_distribution': genres_distribution[:10],
        'rating_distribution': rating_distribution,
        'decades': decades,
        
        # Statistiques détaillées
        'ratings_stats': {
            'min': stats.get('min_rating', 0),
            'avg': stats.get('avg_rating', 0),
            'max': stats.get('max_rating', 10)
        },
        'decades_stats': {
            'most_films_decade': '1990s',
            'most_films_count': 2500,
            'growth': 16.7
        },
        
        # Classements
        'top_movies': top_movies,
        'top_actors': top_actors,
        
        'title': 'Statistiques'
    }
    
    return render(request, 'movies/stats.html', context)
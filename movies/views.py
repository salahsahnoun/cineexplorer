"""
Vues Django pour T3.3
"""
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from .services import sqlite_service, mongo_service

def test_view(request):
    """
    Vue de test principale pour T3.3
    Affiche les connexions aux bases de données
    """
    # Récupérer les statistiques
    sqlite_stats = sqlite_service.get_movie_stats()
    mongo_stats = mongo_service.get_mongo_stats()
    
    # Tester un film spécifique
    test_movie_id = "tt0111161"  # The Shawshank Redemption
    test_movie = mongo_service.get_complete_movie(test_movie_id)
    
    # Préparer le contexte
    context = {
        'sqlite_stats': sqlite_stats,
        'mongo_stats': mongo_stats,
        'test_movie': {
            'id': test_movie_id,
            'found': test_movie is not None,
            'title': test_movie.get('title') if test_movie else 'Non trouvé',
            'year': test_movie.get('year') if test_movie else 'N/A'
        },
        'connections': {
            'sqlite': '✅ OK' if 'error' not in sqlite_stats else '❌ Erreur',
            'mongo': '✅ OK' if mongo_stats.get('replica_status') == 'ok' else '❌ Erreur'
        },
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
        },
        'summary': {
            'sqlite_available': 'error' not in sqlite_stats,
            'mongodb_available': mongo_stats.get('replica_status') == 'ok',
            'has_complete_collection': mongo_stats.get('has_complete_collection', False)
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

import time
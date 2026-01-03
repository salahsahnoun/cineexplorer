"""
URLs de l'application movies pour T3.3
"""
from django.urls import path
from . import views



urlpatterns = [
    path('', views.home_view_phase4, name='home'),
    path('test/', views.test_view, name='test'),
    path('api/test/', views.api_test, name='api_test'),
    path('search/', views.search_view, name='search'),
    path('movies/', views.movie_list_view, name='movie_list'),
    path('movies/<str:movie_id>/', views.movie_detail_view, name='movie_detail'),
    path('stats/', views.stats_view, name='stats'),
    # Route alternative pour l'ancienne version
    path('old-home/', views.home_view, name='old_home'),
]
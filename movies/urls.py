"""
URLs de l'application movies pour T3.3
"""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.home_view, name='home'),
    path('test/', views.test_view, name='test'),
    path('api/test/', views.api_test, name='api_test'),
]
from django.urls import path
from .views import TripsListView

urlpatterns = [
    path('', TripsListView.as_view(), name='trips-list'),
]

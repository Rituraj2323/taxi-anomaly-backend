from django.urls import path
from .views import BatchScoreView

urlpatterns = [
    path('', BatchScoreView.as_view(), name='batch-score'),
]

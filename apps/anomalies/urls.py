from django.urls import path
from .views import (
    AnomalyListView, TopKAnomalyView,
    AnomalyStatsView, ChartDataView, AnomalyDistributionView,
    PassengerCheckView
)

urlpatterns = [
    path('', AnomalyListView.as_view(), name='anomaly-list'),
    path('topk', TopKAnomalyView.as_view(), name='anomaly-topk'),
    path('stats', AnomalyStatsView.as_view(), name='anomaly-stats'),
    path('chart-data', ChartDataView.as_view(), name='anomaly-chart'),
    path('distribution', AnomalyDistributionView.as_view(), name='anomaly-distribution'),
    path('check', PassengerCheckView.as_view(), name='anomaly-check'),
]

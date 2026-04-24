from django.urls import path
from .views import (
    AnomalyListView, TopKAnomalyView,
    AnomalyStatsView, ChartDataView, AnomalyDistributionView,
    PassengerCheckView
)

urlpatterns = [
    path('', AnomalyListView.as_view(), name='anomaly-list'),
    path('topk/', TopKAnomalyView.as_view(), name='anomaly-topk'),
    path('topk', TopKAnomalyView.as_view(), name='anomaly-topk-ns'),
    path('stats/', AnomalyStatsView.as_view(), name='anomaly-stats'),
    path('stats', AnomalyStatsView.as_view(), name='anomaly-stats-ns'),
    path('chart-data/', ChartDataView.as_view(), name='anomaly-chart'),
    path('chart-data', ChartDataView.as_view(), name='anomaly-chart-ns'),
    path('distribution/', AnomalyDistributionView.as_view(), name='anomaly-distribution'),
    path('distribution', AnomalyDistributionView.as_view(), name='anomaly-distribution-ns'),
    path('check/', PassengerCheckView.as_view(), name='anomaly-check'),
    path('check', PassengerCheckView.as_view(), name='anomaly-check-ns'),
]

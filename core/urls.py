from django.urls import path, include

urlpatterns = [
    path('api/auth/', include('apps.authentication.urls')),
    path('api/trips/', include('apps.trips.urls')),
    path('api/anomalies/', include('apps.anomalies.urls')),
    path('api/batch-score', include('apps.batch.urls')),
]

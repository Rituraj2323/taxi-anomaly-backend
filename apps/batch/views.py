from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .services import score_trips


class BatchScoreView(APIView):
    """POST /api/batch-score — Accept JSON trip data, score and store."""

    def post(self, request):
        trips = request.data.get('trips', [])
        if not trips:
            return Response({'error': 'No trips provided.'}, status=400)
        if not isinstance(trips, list):
            return Response({'error': '`trips` must be a list.'}, status=400)
        if len(trips) > 10000:
            return Response({'error': 'Max 10,000 trips per batch.'}, status=400)

        try:
            results = score_trips(trips)
            anomaly_count = sum(1 for r in results if r['is_anomaly'])
            return Response({
                'processed': len(results),
                'anomalies_found': anomaly_count,
                'results': results,
            })
        except FileNotFoundError:
            return Response(
                {'error': 'Model not trained yet. Run the ML pipeline first.'},
                status=503
            )
        except Exception as e:
            return Response({'error': str(e)}, status=500)

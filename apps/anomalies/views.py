from rest_framework.views import APIView
from rest_framework.response import Response
from core.db import get_collection
from apps.batch.services import _load_model
import pandas as pd


class AnomalyListView(APIView):
    """GET /api/anomalies — paginated, filterable anomaly list."""

    def get(self, request):
        anomalies = get_collection('anomalies')
        page = int(request.query_params.get('page', 1))
        page_size = int(request.query_params.get('page_size', 50))
        skip = (page - 1) * page_size

        query = {'is_anomaly': True}

        min_fare = request.query_params.get('min_fare')
        max_fare = request.query_params.get('max_fare')
        if min_fare or max_fare:
            query['fare_amount'] = {}
            if min_fare:
                query['fare_amount']['$gte'] = float(min_fare)
            if max_fare:
                query['fare_amount']['$lte'] = float(max_fare)

        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')
        if date_from or date_to:
            query['pickup_date'] = {}
            if date_from:
                query['pickup_date']['$gte'] = date_from
            if date_to:
                query['pickup_date']['$lte'] = date_to

        zone = request.query_params.get('zone')
        if zone and zone.lower() != 'all':
            query['zone'] = zone

        total = anomalies.count_documents(query)
        cursor = anomalies.find(query, {'_id': 0}).sort(
            'anomaly_score', -1  # Highest % = most anomalous
        ).skip(skip).limit(page_size)
        results = list(cursor)

        return Response({
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': (total + page_size - 1) // page_size,
            'results': results,
        })


class TopKAnomalyView(APIView):
    """GET /api/anomalies/topk?k=50 — Top-K most suspicious trips."""

    def get(self, request):
        anomalies = get_collection('anomalies')
        k = int(request.query_params.get('k', 50))
        k = min(k, 500)  # safety cap

        cursor = anomalies.find(
            {'is_anomaly': True}, {'_id': 0}
        ).sort('anomaly_score', -1).limit(k)  # Descending: highest % first

        return Response({'k': k, 'results': list(cursor)})


class AnomalyStatsView(APIView):
    """GET /api/anomalies/stats — Dashboard KPI metrics."""

    def get(self, request):
        rides = get_collection('rides')
        anomalies = get_collection('anomalies')

        total_trips = rides.count_documents({})
        total_anomalies = anomalies.count_documents({'is_anomaly': True})
        anomaly_rate = round(
            (total_anomalies / total_trips * 100) if total_trips > 0 else 0, 2
        )

        # Average fare across all rides
        pipeline = [{'$group': {'_id': None, 'avg_fare': {'$avg': '$fare_amount'}}}]
        avg_result = list(rides.aggregate(pipeline))
        avg_fare = round(avg_result[0]['avg_fare'], 2) if avg_result else 0

        # Average anomaly score
        score_pipeline = [
            {'$match': {'is_anomaly': True}},
            {'$group': {'_id': None, 'avg_score': {'$avg': '$anomaly_score'}}}
        ]
        score_result = list(anomalies.aggregate(score_pipeline))
        avg_score = round(score_result[0]['avg_score'], 4) if score_result else 0

        return Response({
            'total_trips_analyzed': total_trips,
            'anomalies_detected': total_anomalies,
            'anomaly_rate_percent': anomaly_rate,
            'avg_fare': avg_fare,
            'avg_anomaly_score': avg_score,
            'active_alerts': total_anomalies,
        })


class ChartDataView(APIView):
    """GET /api/anomalies/chart-data — Daily anomaly trend for line chart."""

    def get(self, request):
        rides = get_collection('rides')
        anomalies = get_collection('anomalies')

        # Daily total trips
        total_pipeline = [
            {'$group': {
                '_id': '$pickup_date',
                'total': {'$sum': 1},
                'avg_fare': {'$avg': '$fare_amount'}
            }},
            {'$sort': {'_id': 1}}
        ]
        total_by_day = {r['_id']: r for r in rides.aggregate(total_pipeline)}

        # Daily anomalies
        anomaly_pipeline = [
            {'$match': {'is_anomaly': True}},
            {'$group': {'_id': '$pickup_date', 'anomalies': {'$sum': 1}}},
            {'$sort': {'_id': 1}}
        ]
        anomaly_by_day = {r['_id']: r['anomalies'] for r in anomalies.aggregate(anomaly_pipeline)}

        results = []
        for date, day_data in sorted(total_by_day.items()):
            if not date:
                continue
            total = day_data['total']
            anom = anomaly_by_day.get(date, 0)
            rate = round((anom / total * 100) if total > 0 else 0, 2)
            results.append({
                'date': date,
                'total_trips': total,
                'anomalies': anom,
                'anomaly_rate': rate,
                'avg_fare': round(day_data.get('avg_fare', 0), 2),
            })

        return Response(results)


class AnomalyDistributionView(APIView):
    """GET /api/anomalies/distribution — Normal vs Anomaly counts for pie chart."""

    def get(self, request):
        anomalies = get_collection('anomalies')
        total = anomalies.count_documents({})
        anom = anomalies.count_documents({'is_anomaly': True})
        normal = total - anom
        return Response([
            {'name': 'Normal', 'value': normal},
            {'name': 'Anomaly', 'value': anom},
        ])


class PassengerCheckView(APIView):
    """POST /api/anomalies/check — Real-time stateless inference for a single ride."""

    def post(self, request):
        fare = request.data.get('fare_amount')
        dist = request.data.get('trip_distance')
        passengers = request.data.get('passenger_count', 1)

        if fare is None or dist is None:
            return Response({'error': 'fare_amount and trip_distance are required.'}, status=400)

        # Basic feature engineering matching the training dataset
        try:
            fare = float(fare)
            dist = float(dist)
            passengers = int(passengers)
        except ValueError:
            return Response({'error': 'Invalid numerical inputs.'}, status=400)

        if dist <= 0 or fare <= 0:
            return Response({'error': 'Distance and Fare must be > 0.'}, status=400)

        dist_km = dist * 1.60934
        fare_per_km = fare / max(dist_km, 0.1)
        duration_min = (dist / 15.0) * 60
        fare_per_min = fare / max(duration_min, 0.5)

        # Load model bundle
        try:
            model_data = _load_model()
        except FileNotFoundError:
            return Response({'error': 'Model not trained yet.'}, status=503)

        model = model_data['model']
        scaler = model_data['scaler']
        features = model_data['features']

        # Construct DataFrame EXACTLY matching training features
        # ['fare_amount', 'trip_distance', 'trip_duration_min', 'fare_per_km', 'fare_per_min', 'passenger_count']
        trip_df = pd.DataFrame([{
            'fare_amount': fare,
            'trip_distance': dist,
            'trip_duration_min': duration_min,
            'fare_per_km': fare_per_km,
            'fare_per_min': fare_per_min,
            'passenger_count': passengers
        }])

        X = trip_df[features].fillna(0)
        X_scaled = scaler.transform(X)

        raw_score = float(model.score_samples(X_scaled)[0])
        
        offset = model_data.get('offset', 0)
        max_s = model_data.get('max_s', 0)
        min_s = model_data.get('min_s', -1)
        
        if raw_score >= offset:
            pct = 85.0 * (max_s - raw_score) / (max_s - offset) if max_s > offset else 0.0
        else:
            pct = 85.0 + 15.0 * (offset - raw_score) / (offset - min_s) if offset > min_s else 100.0
            
        # Advanced NYC expected fare baseline (2023 TLC Rules)
        # Base fare ($3.00) + MTA/Improvement/Congestion ($4.00) = $7.00 fixed
        # Mileage: $3.50 per mile
        # Time: Assuming ~50% of duration is spent idle/slow-traffic at $0.70 per min
        expected_fare = 7.00 + (dist * 3.50) + ((duration_min * 0.5) * 0.70)
        
        score = round(min(max(pct, 0.0), 100.0), 2)
        is_anomaly = score >= 85.0
        
        anomaly_type = None
        if is_anomaly:
            if fare > expected_fare * 1.30:  # 30% margin of error for heavy traffic
                anomaly_type = 'overcharge'
            elif fare < expected_fare * 0.70:
                anomaly_type = 'undercharge'
            else:
                anomaly_type = 'unusual_pattern'

        return Response({
            'is_anomaly': is_anomaly,
            'anomaly_type': anomaly_type,
            'expected_fare': round(expected_fare, 2),
            'score': score,
            'breakdown': {
                'fare_per_km': round(fare_per_km, 2),
                'fare_per_min': round(fare_per_min, 2),
                'estimated_duration_min': round(duration_min, 1)
            }
        })


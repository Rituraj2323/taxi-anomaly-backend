import uuid
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from django.conf import settings
from core.db import get_collection


def _load_model():
    with open(settings.ML_MODEL_PATH, 'rb') as f:
        return pickle.load(f)


def score_trips(trip_list: list) -> list:
    """
    Accept a list of trip dicts, engineer features, score with IsolationForest.
    Returns list with anomaly_score and is_anomaly added.
    """
    model_data = _load_model()
    model = model_data['model']
    scaler = model_data['scaler']
    features = model_data['features']

    df = pd.DataFrame(trip_list)

    # Feature engineering
    if 'pickup_time' in df.columns and 'dropoff_time' in df.columns:
        df['pickup_dt'] = pd.to_datetime(df['pickup_time'], errors='coerce')
        df['dropoff_dt'] = pd.to_datetime(df['dropoff_time'], errors='coerce')
        df['trip_duration_min'] = (
            (df['dropoff_dt'] - df['pickup_dt']).dt.total_seconds() / 60
        ).clip(lower=0)
    else:
        df['trip_duration_min'] = 0

    if 'distance_km' in df.columns:
        df['fare_per_km'] = df.apply(
            lambda r: r['fare_amount'] / r['distance_km']
            if r['distance_km'] > 0 else 0, axis=1
        )
    else:
        df['fare_per_km'] = 0

    df['fare_per_min'] = df.apply(
        lambda r: r['fare_amount'] / r['trip_duration_min']
        if r['trip_duration_min'] > 0 else 0, axis=1
    )

    # Score
    X = df[features].fillna(0)
    X_scaled = scaler.transform(X)
    scores = model.score_samples(X_scaled)
    
    offset = model_data.get('offset', 0)
    max_s = model_data.get('max_s', 0)
    min_s = model_data.get('min_s', -1)

    anomalies_col = get_collection('anomalies')
    results = []
    for i, trip in enumerate(trip_list):
        raw = float(scores[i])
        if raw >= offset:
            pct = 85.0 * (max_s - raw) / (max_s - offset) if max_s > offset else 0.0
        else:
            pct = 85.0 + 15.0 * (offset - raw) / (offset - min_s) if offset > min_s else 100.0
            
        score = round(min(max(pct, 0.0), 100.0), 2)
        is_anomaly = score >= 85.0
        anomaly_id = str(uuid.uuid4())

        record = {
            'anomaly_id': anomaly_id,
            'ride_id': trip.get('ride_id', str(uuid.uuid4())),
            'anomaly_score': score,
            'is_anomaly': is_anomaly,
            'pickup_time': trip.get('pickup_time', ''),
            'pickup_date': str(trip.get('pickup_time', ''))[:10],
            'fare_amount': trip.get('fare_amount', 0),
            'distance_km': trip.get('distance_km', 0),
            'fare_per_km': float(df.iloc[i].get('fare_per_km', 0)),
            'fare_per_min': float(df.iloc[i].get('fare_per_min', 0)),
            'trip_duration_min': float(df.iloc[i].get('trip_duration_min', 0)),
            'zone': trip.get('zone', 'Unknown'),
            'created_at': datetime.utcnow().isoformat(),
        }
        anomalies_col.insert_one({k: v for k, v in record.items()})
        results.append({k: v for k, v in record.items()})

    return results

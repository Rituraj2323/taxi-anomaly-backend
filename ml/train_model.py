"""
ML Pipeline: Train Isolation Forest on NYC Yellow Taxi data and seed MongoDB.

Dataset columns available: fare_amount, trip_distance, passenger_count, anomaly
(Pre-processed dataset — no datetime columns present)

Run:
  cd taxi-django-backend && source venv/bin/activate && python ml/train_model.py
"""

import os, sys, uuid, pickle, random, warnings
import numpy as np
import pandas as pd
from datetime import datetime, date
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pymongo

warnings.filterwarnings('ignore')
random.seed(42)

PARQUET_PATH  = "/Users/riturajbhattacharjee/Desktop/yellow_tripdata_2023-02 (1).parquet"
MONGODB_URI   = "mongodb://localhost:27017"
MONGODB_DB    = "taxi_anomaly_db"
CONTAMINATION = 'auto'  # Let the model discover the threshold from data (uses offset=-0.5)
N_ESTIMATORS  = 100
MODEL_OUT     = os.path.join(os.path.dirname(__file__), "isolation_forest.pkl")

FEB_DATES = [date(2023, 2, d) for d in range(1, 29)]

def rand_date(): return str(random.choice(FEB_DATES))
def rand_time(d): return f"{d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:00"


def load_data():
    print(f"[1/5] Loading: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"      Rows: {len(df):,}  |  Columns: {df.columns.tolist()}")
    print("      Processing ENTIRE dataset. This may take a few minutes...")
    return df


def clean_data(df):
    print("[2/5] Cleaning ...")
    mask = (
        (df['fare_amount']   > 0) & (df['fare_amount']   < 500) &
        (df['trip_distance'] > 0) & (df['trip_distance'] < 100)
    )
    if 'passenger_count' in df.columns:
        mask &= df['passenger_count'] > 0
    df = df[mask].dropna(subset=['fare_amount', 'trip_distance']).copy()
    if 'passenger_count' not in df.columns:
        df['passenger_count'] = 1
    print(f"      {len(df):,} rows after cleaning.")
    return df


def engineer_features(df):
    print("[3/5] Engineering features ...")
    df['distance_km']       = df['trip_distance'] * 1.60934
    df['fare_per_km']       = df['fare_amount'] / df['distance_km'].clip(lower=0.1)
    df['trip_duration_min'] = (df['trip_distance'] / 15.0) * 60   # NYC avg 15 mph
    df['fare_per_min']      = df['fare_amount'] / df['trip_duration_min'].clip(lower=0.5)
    
    # Generate dates highly efficiently using numpy random choices
    import numpy as np
    n = len(df)
    fast_dates = np.random.choice([str(d) for d in FEB_DATES], size=n)
    hours = np.random.randint(0, 24, size=n)
    mins = np.random.randint(0, 60, size=n)
    
    # Vectorized string building is much faster for 2.8M rows
    date_series = pd.Series(fast_dates, index=df.index)
    df['pickup_date']  = date_series
    df['pickup_time']  = date_series + ' ' + pd.Series(hours, index=df.index).astype(str).str.zfill(2) + ':' + pd.Series(mins, index=df.index).astype(str).str.zfill(2) + ':00'
    df['dropoff_time'] = df['pickup_time']
    
    # Fast UUID generation
    df['ride_id'] = [str(uuid.uuid4()) for _ in range(n)]
    print("      Done: distance_km, fare_per_km, trip_duration_min, fare_per_min")
    return df


def train_model(df):
    print("[4/5] Training Isolation Forest (This will take a few minutes on ~2.8M rows) ...")
    feats = ['fare_amount', 'trip_distance', 'trip_duration_min',
             'fare_per_km', 'fare_per_min', 'passenger_count']
    X       = df[feats].fillna(0)
    scaler  = StandardScaler()
    Xs      = scaler.fit_transform(X)
    model   = IsolationForest(n_estimators=N_ESTIMATORS, contamination=CONTAMINATION,
                              random_state=42, n_jobs=-1, warm_start=False)
    model.fit(Xs)
    
    print("      Scoring all rows...")
    raw_scores = model.score_samples(Xs)
    
    offset = model.offset_
    max_s = raw_scores.max()
    min_s = raw_scores.min()
    
    # Piecewise scale: Normal (max_s to offset) -> 0 to 85, Anomaly (offset to min_s) -> 85 to 100
    def scale_score(score):
        if score >= offset:
            return 85.0 * (max_s - score) / (max_s - offset) if max_s > offset else 0.0
        else:
            return 85.0 + 15.0 * (offset - score) / (offset - min_s) if offset > min_s else 100.0
            
    vscale = np.vectorize(scale_score)
    df['anomaly_score'] = np.round(vscale(raw_scores), 2)
    df['is_anomaly']    = df['anomaly_score'] >= 85.0
    
    n = int(df['is_anomaly'].sum())
    print(f"      Anomalies: {n:,}/{len(df):,}  ({n/len(df)*100:.1f}%)")
    with open(MODEL_OUT, 'wb') as f:
        pickle.dump({
            'model': model, 
            'scaler': scaler, 
            'features': feats,
            'max_s': float(max_s),
            'min_s': float(min_s),
            'offset': float(offset)
        }, f)
    print(f"      Saved → {MODEL_OUT}")
    return df


def seed_mongodb(df):
    print("[5/5] Seeding MongoDB with Batch Chunking ...")
    client = pymongo.MongoClient(MONGODB_URI)
    db     = client[MONGODB_DB]
    db['rides'].drop()
    db['anomalies'].drop()
    
    batch_size = 50000
    total_rows = len(df)
    now = datetime.utcnow().isoformat()
    
    print(f"      Writing {total_rows:,} records in batches of {batch_size}...")
    
    for start in range(0, total_rows, batch_size):
        chunk = df.iloc[start:start+batch_size]
        ride_docs, anom_docs = [], []
        
        for _, r in chunk.iterrows():
            rid = r['ride_id']
            ride_docs.append({
                'ride_id': rid, 'vendor_id': 1,
                'pickup_time': r['pickup_time'], 'dropoff_time': r['dropoff_time'],
                'pickup_date': r['pickup_date'], 'passenger_count': int(r['passenger_count']),
                'trip_distance': round(float(r['trip_distance']), 4),
                'distance_km': round(float(r['distance_km']), 4),
                'fare_amount': round(float(r['fare_amount']), 2),
                'trip_duration_min': round(float(r['trip_duration_min']), 2),
                'fare_per_km': round(float(r['fare_per_km']), 4),
                'fare_per_min': round(float(r['fare_per_min']), 4),
                'zone': 'NYC',
            })
            anom_docs.append({
                'anomaly_id': str(uuid.uuid4()), 'ride_id': rid,
                'anomaly_score': round(float(r['anomaly_score']), 6),
                'is_anomaly': bool(r['is_anomaly']),
                'pickup_time': r['pickup_time'], 'pickup_date': r['pickup_date'],
                'fare_amount': round(float(r['fare_amount']), 2),
                'distance_km': round(float(r['distance_km']), 4),
                'trip_duration_min': round(float(r['trip_duration_min']), 2),
                'fare_per_km': round(float(r['fare_per_km']), 4),
                'fare_per_min': round(float(r['fare_per_min']), 4),
                'passenger_count': int(r['passenger_count']),
                'zone': 'NYC', 'created_at': now,
            })

        db['rides'].insert_many(ride_docs, ordered=False)
        db['anomalies'].insert_many(anom_docs, ordered=False)
        sys.stdout.write(f"\r      Progress: {min(start+batch_size, total_rows):,} / {total_rows:,}")
        sys.stdout.flush()
        
    print("\n      Creating Indexes...")
    for col, fields in [('rides', ['ride_id','pickup_date','fare_amount']),
                        ('anomalies', ['ride_id','anomaly_score','is_anomaly','pickup_date'])]:
        for f in fields:
            db[col].create_index(f)
    print("      Indexes created.")
    client.close()


def main():
    if not os.path.exists(PARQUET_PATH):
        print(f"ERROR: File not found: {PARQUET_PATH}"); sys.exit(1)
    df = load_data()
    df = clean_data(df)
    df = engineer_features(df)
    df = train_model(df)
    seed_mongodb(df)
    print(f"\n✅ Done!  rides={len(df):,}  anomalies={int(df['is_anomaly'].sum()):,}")

if __name__ == '__main__':
    main()

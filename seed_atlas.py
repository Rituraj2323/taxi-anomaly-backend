"""
Seed MongoDB Atlas from local MongoDB.

Copies a representative subset of 'rides' and 'anomalies' from local
MongoDB to Atlas. Uses 100K rides (sampled) + ALL anomalies matching
those rides, which fits within the Atlas M0 512 MB free tier.

Usage:
  cd taxi-django-backend
  source venv/bin/activate
  python seed_atlas.py "mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/taxi_anomaly_db?retryWrites=true&w=majority"
"""

import sys
import pymongo

LOCAL_URI = "mongodb://localhost:27017"
LOCAL_DB  = "taxi_anomaly_db"
BATCH     = 5000
MAX_RIDES = 100_000  # Subset size — fits M0 free tier (512 MB)

def main():
    if len(sys.argv) < 2:
        print("Usage: python seed_atlas.py <ATLAS_MONGODB_URI>")
        sys.exit(1)

    atlas_uri = sys.argv[1]
    print(f"[*] Connecting to local MongoDB ({LOCAL_URI}) ...")
    local_client = pymongo.MongoClient(LOCAL_URI)
    local_db = local_client[LOCAL_DB]

    print(f"[*] Connecting to Atlas ...")
    atlas_client = pymongo.MongoClient(atlas_uri)
    atlas_db_name = atlas_uri.split('/')[-1].split('?')[0] or LOCAL_DB
    atlas_db = atlas_client[atlas_db_name]

    try:
        atlas_client.admin.command('ping')
        print("    ✅ Atlas connected!")
    except Exception as e:
        print(f"    ❌ Atlas connection failed: {e}")
        sys.exit(1)

    # ── Step 1: Seed rides (sampled subset) ──────────────────────────
    print(f"\n[1/4] Seeding rides (up to {MAX_RIDES:,} sampled from local) ...")
    local_rides = local_db['rides']
    atlas_rides = atlas_db['rides']
    local_total = local_rides.count_documents({})

    atlas_rides.drop()
    print(f"    Local rides: {local_total:,}")

    # Use $sample aggregation for a random representative subset
    pipeline = [{'$sample': {'size': MAX_RIDES}}, {'$project': {'_id': 0}}]
    batch = []
    inserted = 0
    ride_ids = set()

    for doc in local_rides.aggregate(pipeline, allowDiskUse=True):
        ride_ids.add(doc.get('ride_id'))
        batch.append(doc)
        if len(batch) >= BATCH:
            atlas_rides.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []
            sys.stdout.write(f"\r    Rides inserted: {inserted:,} / {MAX_RIDES:,}")
            sys.stdout.flush()

    if batch:
        atlas_rides.insert_many(batch, ordered=False)
        inserted += len(batch)

    print(f"\r    Rides inserted: {inserted:,} / {MAX_RIDES:,}")

    # ── Step 2: Seed matching anomalies ──────────────────────────────
    print(f"\n[2/4] Seeding anomalies (matching {len(ride_ids):,} ride_ids) ...")
    local_anomalies = local_db['anomalies']
    atlas_anomalies = atlas_db['anomalies']
    atlas_anomalies.drop()

    # Query anomalies that match our sampled rides
    ride_id_list = list(ride_ids)
    batch = []
    inserted = 0

    # Process in chunks of ride_ids to avoid huge $in queries
    chunk_size = 10000
    for chunk_start in range(0, len(ride_id_list), chunk_size):
        chunk_ids = ride_id_list[chunk_start:chunk_start + chunk_size]
        for doc in local_anomalies.find(
            {'ride_id': {'$in': chunk_ids}},
            {'_id': 0}
        ):
            batch.append(doc)
            if len(batch) >= BATCH:
                atlas_anomalies.insert_many(batch, ordered=False)
                inserted += len(batch)
                batch = []
                sys.stdout.write(f"\r    Anomalies inserted: {inserted:,}")
                sys.stdout.flush()

    if batch:
        atlas_anomalies.insert_many(batch, ordered=False)
        inserted += len(batch)

    print(f"\r    Anomalies inserted: {inserted:,}")

    # ── Step 3: Create indexes ───────────────────────────────────────
    print(f"\n[3/4] Creating indexes ...")
    for f in ['ride_id', 'pickup_date', 'fare_amount']:
        atlas_rides.create_index(f)
    for f in ['ride_id', 'anomaly_score', 'is_anomaly', 'pickup_date']:
        atlas_anomalies.create_index(f)
    print("    ✅ Indexes created.")

    # ── Step 4: Sync users ───────────────────────────────────────────
    if 'users' in local_db.list_collection_names():
        print(f"\n[4/4] Syncing users ...")
        local_users = local_db['users']
        atlas_users = atlas_db['users']
        user_count = local_users.count_documents({})
        if user_count > 0:
            atlas_users.drop()
            users = list(local_users.find({}, {'_id': 0}))
            atlas_users.insert_many(users)
            print(f"    ✅ {len(users)} users synced.")
    else:
        print(f"\n[4/4] No users collection, skipping.")

    # ── Summary ──────────────────────────────────────────────────────
    final_rides = atlas_rides.count_documents({})
    final_anomalies = atlas_anomalies.count_documents({})
    final_anom_flagged = atlas_anomalies.count_documents({'is_anomaly': True})
    print(f"\n🎉 Atlas seeded successfully!")
    print(f"   rides:     {final_rides:,}")
    print(f"   anomalies: {final_anomalies:,} ({final_anom_flagged:,} flagged)")
    print(f"\n   Make sure MONGODB_URI on Render is set to your Atlas connection string.")

    atlas_client.close()
    local_client.close()

if __name__ == '__main__':
    main()

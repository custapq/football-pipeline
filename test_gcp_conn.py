from google.cloud import storage, bigquery

def test_gcs():
    client = storage.Client()
    buckets = list(client.list_buckets())
    print(f"✅ GCS Connected — Found {len(buckets)} buckets")
    for b in buckets:
        print(f"   - {b.name}")

def test_bigquery():
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    print(f"✅ BigQuery Connected — Found {len(datasets)} datasets")
    for d in datasets:
        print(f"   - {d.dataset_id}")

if __name__ == "__main__":
    test_gcs()
    test_bigquery()

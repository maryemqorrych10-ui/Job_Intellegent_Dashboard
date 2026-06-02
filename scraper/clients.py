# =============================================================================
#  scraper/clients.py — Initialisation des clients Kafka et MinIO
# =============================================================================


from minio import Minio
from config import (
    
    MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET,
)



def get_minio_client() -> Minio:
    """Retourne un client MinIO configuré."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )
from config import MINIO_BUCKET_RAW, MINIO_BUCKET_SILVER, MINIO_BUCKET_GOLD

def init_minio_buckets(client=None):
    if client is None:
        client = get_minio_client()
    from config import MINIO_BUCKET_RAW, MINIO_BUCKET_SILVER, MINIO_BUCKET_GOLD
    for bucket in [MINIO_BUCKET_RAW, MINIO_BUCKET_SILVER, MINIO_BUCKET_GOLD]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"✅ Bucket {bucket} créé")



def get_mock_minio_client():
    """Mock client pour les tests"""
    class MockMinIO:
        def put_object(self, bucket, object_name, data, length, content_type):
            print(f"[MOCK] Saving to {bucket}/{object_name}")
    return MockMinIO()
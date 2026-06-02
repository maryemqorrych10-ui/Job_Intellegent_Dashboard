# =============================================================================
#  scraper/clients.py — Initialisation des clients Kafka et MinIO
# =============================================================================

from confluent_kafka import Producer
from minio import Minio
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET,
)


def get_kafka_producer() -> Producer:
    """Retourne un producteur Kafka configuré."""
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


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

def get_mock_kafka_producer():
    """Mock producer pour les tests"""
    class MockProducer:
        def produce(self, topic, key, value):
            print(f"[MOCK] Producing to {topic}: {value[:100]}...")
        def flush(self):
            pass
    return MockProducer()

def get_mock_minio_client():
    """Mock client pour les tests"""
    class MockMinIO:
        def put_object(self, bucket, object_name, data, length, content_type):
            print(f"[MOCK] Saving to {bucket}/{object_name}")
    return MockMinIO()
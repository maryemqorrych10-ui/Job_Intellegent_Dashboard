# scripts/etl_from_minio.py
import sys
import os
import json
from datetime import datetime
from minio import Minio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET
from etl_transform import transformer_offre, dedupliquer, save_silver_minio, save_gold_minio
from load_star_schema import load_offres_to_star_schema

def get_minio_client():
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

def read_raw_offres_from_minio(bucket="raw", sources=None):
    """Lit tous les fichiers JSON du bucket raw (bronze) pour les sources données."""
    if sources is None:
        sources = ["rekrute", "france_travail", "adzuna"]
    client = get_minio_client()
    all_offres = []
    for src in sources:
        prefix = f"{src}/"
        try:
            objects = client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objects:
                data = client.get_object(bucket, obj.object_name)
                content = json.loads(data.read().decode("utf-8"))
                data.close()
                if isinstance(content, list):
                    all_offres.extend(content)
                else:
                    all_offres.append(content)
                print(f"[ETL] {obj.object_name} : {len(content) if isinstance(content, list) else 1} offres")
        except Exception as e:
            print(f"[ETL] Erreur lecture {src} : {e}")
    return all_offres

def run_etl_from_minio(batch_size=100):
    """Lit toutes les offres brutes depuis MinIO, les transforme par lots et charge."""
    print("🚀 ETL depuis MinIO (bronze → silver/gold → MySQL)")
    offres_brutes = read_raw_offres_from_minio()
    if not offres_brutes:
        print("⚠️ Aucune offre brute trouvée dans MinIO")
        return {"total_lus": 0, "total_charge": 0}
    
    print(f"📥 {len(offres_brutes)} offres brutes lues")
    
    # Transformation par lots
    buffer = []
    total_charge = 0
    for offre in offres_brutes:
        t = transformer_offre(offre)
        if t:
            buffer.append(t)
        if len(buffer) >= batch_size:
            clean, _ = dedupliquer(buffer)
            if clean:
                # Sauvegarde silver/gold
                sources = set(o["source"] for o in clean)
                for src in sources:
                    src_offres = [o for o in clean if o["source"] == src]
                    save_silver_minio(src_offres, src)
                save_gold_minio(clean)
                # Chargement MySQL
                load_offres_to_star_schema(clean)
                total_charge += len(clean)
            buffer = []
    # Dernier lot
    if buffer:
        clean, _ = dedupliquer(buffer)
        if clean:
            sources = set(o["source"] for o in clean)
            for src in sources:
                src_offres = [o for o in clean if o["source"] == src]
                save_silver_minio(src_offres, src)
            save_gold_minio(clean)
            load_offres_to_star_schema(clean)
            total_charge += len(clean)
    
    print(f"✅ ETL terminé. Offres transformées et chargées : {total_charge}")
    return {"total_lus": len(offres_brutes), "total_charge": total_charge}

if __name__ == "__main__":
    run_etl_from_minio()
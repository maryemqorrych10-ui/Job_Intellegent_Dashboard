# scripts/check_mysql.py
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM fact_offres"))
    count = result.scalar()
    print(f"📊 Nombre total d'offres dans fact_offres : {count}")

    result = conn.execute(text("SELECT COUNT(*) FROM dim_competence"))
    print(f"🏷️  Nombre de compétences distinctes : {result.scalar()}")

    result = conn.execute(text("SELECT source_id, COUNT(*) FROM fact_offres GROUP BY source_id"))
    for row in result:
        print(f"   Source {row[0]} : {row[1]} offres")
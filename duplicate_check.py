import db_connect
from sqlalchemy import text
import csv


engine = db_connect.get_db_engine()
try:

    with open("duplicates.log") as f:
        query = text("""
        SELECT *
        FROM gene_name_translation
        WHERE gene_name = :gene_name AND gene_id = :gene_id
        """)
        reader = csv.reader(f)
        header = True
        for line in reader:
            if header:
                header = False
                continue
            
            fields = {
                "gene_name": line[0],
                "gene_id": line[1]
            }

            res = None
            with engine.begin() as con:
                res = con.execute(query, fields)
            
            print(line)
            print(res.fetchall())

            break

    query = text("SELECT")
finally:
    db_connect.cleanup_db_engine()
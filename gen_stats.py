import sqlite3
from sqlalchemy import text
import db_connect

def filter_single_factory(cursor, row):
    return row[0]

def main():
    dbf = "E:/ncbigeo/GEOmetadb.sqlite"
    con = sqlite3.connect(dbf)
    con.row_factory = filter_single_factory
    query = """
    SELECT gpl
    FROM gpl
    """
    cur = con.cursor()
    res = cur.execute(query)
    gpls = res.fetchall()
    print(len(gpls))
    engine = db_connect.get_db_engine()
    gene_ids = None
    try:  
        query = text("SELECT DISTINCT gene_id FROM gene_name_translation;")
        with engine.begin() as con:
            gene_ids = con.execute(query)
        gene_ids = gene_ids.fetchall()
        print(len(gene_ids))
        i = 0
        for gpl in gpls:
            for gene_id in gene_ids:
                i += i
        print(i)

    finally:
        db_connect.cleanup_db_engine()
        
        

        

        
if __name__ == "__main__":
    main()
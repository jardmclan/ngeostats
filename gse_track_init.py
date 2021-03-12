from db_connect import DBConnector
import sqlite3
from sqlalchemy import text

config = ''

def create_tables(connector):
    query = text("""CREATE TABLE IF NOT EXISTS gse_gpl_processed (
        gse TEXT NOT NULL,
        gpl TEXT NOT NULL,
        processed BOOLEAN,
        PRIMARY KEY (gse(255), gpl(255))
    );""")

    connector.engine_exec(query, None, 0)

def insert_batch(connector, batch):
    query = text("INSERT IGNORE INTO gse_gpl_processed (gse, gpl, processed) VALUES (:gse, :gpl, :processed)")
    connector.engine_exec(query, batch, 5)

def get_gse_gpls(dbf):
    query = "SELECT gse, gpl FROM gse_gpl"
    res = None
    with sqlite3.connect(dbf) as con:
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()
    return res


dbf = "C:/GEOmetadb.sqlite"
batch_size = 10000
batch = []
with DBConnector() as connector:
    create_tables(connector)
    gse_gpls = get_gse_gpls(dbf)
    for gse_gpl in gse_gpls:
        gse = gse_gpl[0]
        gpl = gse_gpl[1]
        row_info = {
            "gse": gse,
            "gpl": gpl,
            "processed": False
        }
        batch.append(row_info)
        if len(batch) >= batch_size:
            insert_batch(connector, batch)
            batch = []
    insert_batch(connector, batch)


    




from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
from db_connect import DBConnector
import sqlite3
from mpi4py.futures import MPIPoolExecutor
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from threading import Semaphore
from ftp_handler import FTPHandler
import gse_gpl_processor
from sys import argv, stderr
from sqlalchemy import text
from json import load


#############################

#!!!A SERIES CAN HAVE MULTIPLE PLATFORMS (CONSTITUENT SAMPLES HAVE DIFFERENT PLATFORMS)
#note that while a sample can potentially be in multiple series the value should always be the same for a given sample and ref_id, so some will be overwritten with the same value

#note can't name a column "values"
def create_gsm_val_table(connector):
    query = """CREATE TABLE IF NOT EXISTS gsm_gene_vals (
        gsm varchar(255) NOT NULL,
        gene_id varchar(255) NOT NULL,  
        expression_values varchar(65535) NOT NULL,
        PRIMARY KEY (gene_id, gsm)
    );"""
    connector.engine_exec(query, None, 0)


def mark_gse_gpl_processed(connector, gse, gpl, retry):
    query = text("""
        UPDATE gse_gpl_processed
        SET processed = true
        WHERE gpl = :gpl AND gse = :gse;
    """)
    params = {
        "gpl": gpl,
        "gse": gse
    }
    connector.engine_exec(query, params, retry)


def get_gse_gpls(connector, retry):
    def row_to_dict(row):
        d = {
            "gse": row[0],
            "gpl": row[1]
        }
        return d
    query = text("SELECT gse, gpl FROM gse_gpl_processed WHERE processed = false")
    res = connector.engine_exec(query, None, retry).fetchall()
    res = list(map(row_to_dict, res))
    return res


def get_gpl_id_ref_map(connector, gpl, retry):
    query = text("SELECT ref_id, gene_id FROM gene_gpl_ref_new WHERE gpl = :gpl")
    params = {
        "gpl": gpl
    }
    res = connector.engine_exec(query, params, retry)
    #want to create id mapping for handle_gse_gpl method which is row_id to gene_id map
    id_ref_map = {}
    for row in res:
        id_ref_map[row[0]] = row[1]
    return id_ref_map



def handle_chunk(chunk, config):
    threads = config["general"]["threads"]
    ftp_base = config["ftp_config"]["ftp_base"]
    ftp_pool_size = config["ftp_config"]["ftp_pool_size"]
    ftp_opts = config["ftp_config"]["ftp_opts"]
    ftp_retry = config["general"]["ftp_retry"]
    db_retry = config["general"]["db_retry"]
    batch_size = config["general"]["insert_batch_size"]
    db_config = config["extern_db_config"]

    def handle_complete(connector, gse, gpl):
        try:
            #mark as processed
            mark_gse_gpl_processed(connector, gse, gpl, db_retry)
        #catch exceptions and print error
        except Exception as e:
            print("An error occured while updating processed entry for gse: %s, gpl: %s: %s" % (gse, gpl, e), file = stderr)
        else:
            #success
            print("Successfully processed gse: %s, gpl: %s" % (gse, gpl))

    def cb(connector, gse, gpl):
        def _cb(f):
            e = f.exception()
            #print exceptions that occured durring processing
            if e:
                print("An error occured processing gse: %s, gpl: %s: %s" % (gse, gpl, e), file = stderr)
            else:
                handle_complete(connector, gse, gpl)
        return _cb

    
    with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
        with DBConnector(db_config) as connector:
            with ThreadPoolExecutor(threads) as t_exec:
                for gse_gpl in chunk:
                    gse = gse_gpl["gse"]
                    gpl = gse_gpl["gpl"]
                    #get row mappings
                    id_ref_map = get_gpl_id_ref_map(connector, gpl, db_retry)
                    #no rows, mark as processed and skip
                    if len(id_ref_map) < 1:
                        handle_complete(connector, gse, gpl)
                    else:
                        f = t_exec.submit(gse_gpl_processor.handle_gse_gpl, connector, ftp_handler, gse, gpl, id_ref_map, db_retry, ftp_retry, batch_size)
                        f.add_done_callback(cb(connector, gse, gpl))
                




def main():
    if len(argv) < 2:
        raise RuntimeError("Invalid command line args. Must provide config file")
    config_file = argv[1]
    config = None
    with open(config_file) as f:
        config = load(f)
    
    def cb(chunk_start, chunk_end):
        def _cb(f):
            e = f.exception()
            if e:
                print("An error occured while processing chunk %d - %d: %s" % (chunk_start, chunk_end, e), file = stderr)
        return _cb
    
    #vars, from config
    # dbf = config["local_db_config"]["meta_db_file"]
    db_retry = config["general"]["db_retry"]
    mpi_procs = config["general"]["mpi_procs"]
    chunk_size = config["general"]["chunk_size"]
    db_config = config["extern_db_config"]

    gse_gpls = None
    with DBConnector(db_config) as connector:
        create_gsm_val_table(connector)
        gse_gpls = get_gse_gpls(connector, db_retry)[:1]
    with ProcessPoolExecutor(mpi_procs) as mpi_exec:
        chunk_start = 0
        chunk_end = 0
        while chunk_end < len(gse_gpls):
            chunk_end = chunk_start + chunk_size
            if chunk_end > len(gse_gpls):
                chunk_end = len(gse_gpls)
            chunk = gse_gpls[chunk_start : chunk_end]
            #need to do anything on return? maybe error handling?
            f = mpi_exec.submit(handle_chunk, chunk, config)
            f.add_done_callback(cb(chunk_start, chunk_end))
            chunk_start = chunk_end
    print("Complete!")






        #get list of unique platforms from translation table
        #for each platform get series from metadata db; get (gene_id, row_id)s from translation table
        #get series values for rows with row_ids in set of (gene_id, row_id)s



if __name__ == "__main__":
    main()













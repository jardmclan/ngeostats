
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
from db_connect import DBConnector
import sqlite3
from mpi4py.futures import MPIPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore
from ftp_handler import FTPHandler
import gse_gpl_processor
from sys import argv, stderr
from sqlalchemy import text
from json import load

# class QueryParamGen():
#     def __init__(self, source, nodata):
#         self.source = source
#         self.nodata = nodata

#     def __next__(self):
#         row = next(self.source)
#         for i in range(len(row)):
#             #some data seems to have weird trailing newlines
#             row[i] = row[i].strip()
#             if row[i] == self.nodata:
#                 row[i] = None
#         return row

# def create_gse_val_table(connector):
#     query = """CREATE TABLE IF NOT EXISTS gene_vals (
#         gene_id TEXT NOT NULL,
#         gpl TEXT NOT NULL,
#         gse TEXT NOT NULL,
#         gsm TEXT NOT NULL,
#         ref_id TEXT NOT NULL,
#         value TEXT NOT NULL,
#         PRIMARY KEY (gene_id, gse, gsm, ref_id)
#     );"""
#     connector.engine_exec(query, None, 0)

# #translate gene names (synonyms) to main symbol (should include main symbol name translated to itself for simplicity)
# def create_gene_syn_table(cur):
#     query = """CREATE TABLE IF NOT EXISTS name2symbol (
#         synonym TEXT NOT NULL PRIMARY KEY
#         symbol TEXT NOT NULL
#     );"""

#     cur.execute(g2a_query)

# def create_gene_stat_table(cur):

#     #use the set of ids as primary key (gene by itself might map to multiples and some ids might be null)
#     query = """CREATE TABLE IF NOT EXISTS genestats (
#         gene_id TEXT NOT NULL
#         gsm TEXT NOT NULL,
#         log2rat number NOT NULL,
#         nlog10p number NOT NULL,
#         PRIMARY KEY (gene_id, gsm)
#     );"""

#     cur.execute(g2a_query)


# p_executor.submit(getData, gpl, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock, translator)







# # def get_stats_from_genes(genes):
# #     p_pool = ProcessPoolExecutor(p_max)
# #     for gene in genes:
# #         p.submit(get_stats_from_gene, gene)


# # def get_stats_from_gene(gene):
# #     t_pool = ThreadPoolExecutor(t_max)
# #     query = "%s%s" % (api_base, endpoints["sym2info"])
# #     gene_info = requests.get()


# def get_gene_row_ids(connector, gpl):
#     query = text("SELECT ref_id, gene_id FROM gene_gpl_ref_new WHERE gpl = ':gpl'")
#     res = connector.engine_exec(query, {"gpl": gpl}, 0)
#     return res

# def get_gses_from_gpl(connector, gpl):
#     query = text("SELECT gse FROM gse_gpl WHERE gpl = ':gpl'")
#     res = connector.engine_exec(query, {"gpl": gpl}, 0)
#     return res

# def get_gses_from_gpl(dbf, gpl):
#     query = ("SELECT gse FROM gse_gpl WHERE gpl = ?")
#     con = sqlite3.connect(dbf)
#     res = con.execute(query, (gpl,))
#     return res


# def get_gpls_from_gse(connector, gse):
#     query = text("SELECT DISTINCT gpl FROM gse_gpl WHERE gse = ':gse'")
#     res = connector.engine_exec(query, {"gse": gse}, 0)
#     return res

# #10 procs, 19 threads per proc (1 main, plus 4 connections, plus some post processing, plus 4 heartbeat, plus extras since can only do 2 per node anyway (19 is half of node))
# #5 nodes, 2 tasks per node, 19 threads per task
# #4 ftp connections per task

# #want to still use some threading due to ability to use ftp connection pool
# def handle_gse_gpl_batch(batch):
#     threads = 18
#     with DBConnector(db_config) as connector:
#         with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
#             with ThreadPoolExecutor(threads) as t_exec:
#                 for item in batch:
#                     gse = item[0]
#                     gpl = item[1]
#                     ids = item[2]
#                     f = t_exec.submit(handle_gse_gpl, connector, ftp_handler, gse, gpl, ids)
#                     def cb(gse, gpl):
#                         def _cb(f):
#                             e = f.exception()
#                             if e is not None:
#                                 e = "Error in gse: %s, gpl: %s handler: %s" % (gse, gpl, str(e))
#                                 print(e, file = stderr)
#                             else:
#                                 try:
#                                     mark_gse_gpl_processed(connector, gse, gpl)
#                                     print("Complete gse: %s, gpl: %s" % (gse, gpl))
#                                 except Exception as e:
#                                     e = "Error while updating gse: %s, gpl: %s processed entry: %s" % (gse, gpl, str(e))
#                                     print(e, file = stderr)
#                         return _cb(f)
#                     f.add_done_callback(cb(gse, gpl))
        
        



# #vars from config
# def gse_gpl_process(gse, gpl, ids):

#     ftp_opts = config["ftp_opts"]
#     ftp_base = config["ftp_base"]
#     ftp_pool_size = config["ftp_pool_size"]
#     g2a_db = config["gene2accession_file"]
    
#     #also one engine for all threads
#     #just use engine exec for everything instead of passing around engine
#     #db_connect.create_db_engine(config["extern_db_config"])
#     insert_batch_size = config["insert_batch_size"]
#     db_retry = config["db_retry"]
#     ftp_retry = config["ftp_retry"]

#     with DBConnector(db_config) as connector:
#         with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
#             handle_gse_gpl()



#############################

#!!!A SERIES CAN HAVE MULTIPLE PLATFORMS (CONSTITUENT SAMPLES HAVE DIFFERENT PLATFORMS)
#note that while a sample can potentially be in multiple series the value should always be the same for a given sample and ref_id, so some will be overwritten with the same value

def create_gsm_val_table(connector):
    query = """CREATE TABLE IF NOT EXISTS gsm_gene_vals (
        gsm varchar(255) NOT NULL,
        gene_id varchar(255) NOT NULL,  
        values varchar(65535) NOT NULL,
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


#connector, ftp_handler, gse, gpl, ids, db_retry, ftp_retry, batch_size
def handle_chunk(chunk, config):
    threads = config["general"]["threads"]
    ftp_base = config["ftp_config"]["ftp_base"]
    ftp_pool_size = config["ftp_config"]["ftp_pool_size"]
    ftp_opts = config["ftp_config"]["ftp_opts"]
    ftp_retry = config["general"]["ftp_retry"]
    db_retry = config["general"]["db_retry"]
    batch_size = config["general"]["insert_batch_size"]
    db_config = config["extern_db_config"]

    def cb(gse, gpl):
        def _cb(f):
            e = f.exception()
            #print exceptions that occured durring processing
            if e:
                print("An error occured processing gse: %s, gpl: %s: %s" % (gse, gpl, e), file = stderr)
            else:
                try:
                    #mark as processed
                    mark_gse_gpl_processed(connector, gse, gpl, db_retry)
                #catch exceptions and print error
                except Exception as e:
                    print("An error occured while updating processed entry for gse: %s, gpl: %s: %s" % (gse, gpl, e), file = stderr)
                else:
                    #success
                    print("Successfully processed gse: %s, gpl: %s" % (gse, gpl))
        return _cb

    
    with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
        with DBConnector(db_config) as connector:
            with ThreadPoolExecutor(threads) as t_exec:
                for gse_gpl in chunk:
                    gse = gse_gpl["gse"]
                    gpl = gse_gpl["gpl"]
                    #get row mappings
                    id_ref_map = get_gpl_id_ref_map(connector, gpl)
                    #no rows, skip
                    if len(id_ref_map) < 1:
                        continue
                    f = t_exec.submit(gse_gpl_processor.handle_gse_gpl, connector, ftp_handler, gse, gpl, id_ref_map, db_retry, ftp_retry, batch_size)
                    f.add_done_callback(cb(gse, gpl))
                






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
    dbf = config["local_db_config"]["meta_db_file"]
    db_retry = config["general"]["db_retry"]
    mpi_procs = config["general"]["mpi_procs"]
    chunk_size = config["general"]["chunk_size"]
    db_config = config["extern_db_config"]

    gse_gpls = None
    with DBConnector(db_config) as connector:
        create_gsm_val_table(connector)
        gse_gpls = get_gse_gpls(connector, db_retry)
    with MPIPoolExecutor(mpi_procs) as mpi_exec:
        chunk_start = 0
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













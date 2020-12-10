#note a series can have multiple platforms (constituent samples have different platforms)
#note that while a sample can potentially be in multiple series the value should always be the same for a given sample and ref_id, so some will be overwritten with the same value

import mpi4py
mpi4py.rc.recv_mprobe = False

from concurrent.futures import ThreadPoolExecutor
from db_connect import DBConnector, DBConnectorError
from ftp_handler import FTPHandler
import gse_gpl_processor
from sys import argv, stderr
from sqlalchemy import text
from json import load
from mpi4py import MPI

comm = MPI.COMM_WORLD

############## statics #####################

distributor_rank = 0

#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = load(f)

############################################


########### per process globals ############

#process rank
rank = comm.Get_rank()
node = MPI.Get_processor_name()

############################################


############ helper functs #################

#note can't name a column "values"
#note default char encoding latin1, which is 1 byte per char (and will be sufficient for these fields)
#values field may have many values, varchar not sufficient for storage
def create_gsm_val_table(connector):
    query = text("""CREATE TABLE IF NOT EXISTS gsm_gene_vals (
        gsm varchar(255) NOT NULL,
        gene_id varchar(255) NOT NULL,  
        expression_values MEDIUMTEXT NOT NULL,
        PRIMARY KEY (gene_id, gsm)
    );""")
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

############################################


########### main handlers ##################


def distribute():
    #distributor rank is not used for processing (only n - 1 processing ranks)
    ranks = comm.Get_size() - 1
   
    ################# config #####################

    db_retry = config["general"]["db_retry"]
    mpi_procs = config["general"]["mpi_procs"]
    chunk_size = config["general"]["chunk_size"]
    db_config = config["extern_db_config"]

    #############################################

    data = None
    with DBConnector(db_config) as connector:
        create_gsm_val_table(connector)
        data = get_gse_gpls(connector, db_retry)[:20]
    chunk_start = 0
    chunk_end = 0
    #distribute chunks until reach end of data or all data handler ranks error out
    while chunk_end < len(data) and ranks > 0:
        chunk_end = chunk_start + chunk_size
        if chunk_end > len(data):
            chunk_end = len(data)
        chunk = data[chunk_start : chunk_end]

        recv_rank = -1
        #get next request for data (continue until receive request or all ranks error out and send -1)
        while recv_rank == -1 and ranks > 0:
            #receive data requests from ranks
            recv_rank = comm.recv()
            #if recv -1 one of the ranks errored out, subtract from processor ranks (won't be requesting any more data)
            if recv_rank == -1:
                ranks -= 1
            #otherwise send data chunk to the rank that requested data
            else:
                comm.send(chunk, dest = recv_rank)

        chunk_start = chunk_end
    print("Data distribution complete. Sending terminators...")
    #while there are ranks that have not received terminator, receive ranks and send terminator
    while ranks > 0:
        recv_rank = comm.recv()
        #send terminator
        comm.send(None, dest = recv_rank)
        #reduce number of ranks that haven't received terminator
        ranks -= 1
    #if every processor requested data and received terminator, all done
    print("Complete!")

        


    

def handle_data():

    ############# helper functs ##################

    def handle_complete(connector, gse, gpl):
        try:
            #mark as processed
            mark_gse_gpl_processed(connector, gse, gpl, db_retry)
        #catch exceptions and print error
        except DBConnectorError as e:
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


    ##############################################

    ################# config #####################

    threads = config["general"]["threads"]
    ftp_base = config["ftp_config"]["ftp_base"]
    ftp_pool_size = config["ftp_config"]["ftp_pool_size"]
    ftp_opts = config["ftp_config"]["ftp_opts"]
    ftp_retry = config["general"]["ftp_retry"]
    db_retry = config["general"]["db_retry"]
    batch_size = config["general"]["insert_batch_size"]
    db_config = config["extern_db_config"]

    ##############################################
    try:
        with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
            with DBConnector(db_config) as connector:
                #send rank to request data
                data = comm.sendrecv(rank, dest = distributor_rank)
                #process data and request more until terminator received from distributor
                while data is not None:
                    #process data
                    with ThreadPoolExecutor(threads) as t_exec:
                        for gse_gpl in data:
                            gse = gse_gpl["gse"]
                            gpl = gse_gpl["gpl"]
                            #get row mappings
                            id_ref_map = None
                            try:
                                id_ref_map = get_gpl_id_ref_map(connector, gpl, db_retry)
                            #don't fail entire processor if a db error occurs while retreiving id_ref_map
                            except DBConnectorError as e:
                                print("A database error occured while retreiving the id reference map for gse: %s, gpl: %s: %s" % (gse, gpl, e), file = stderr)
                            else:
                                #no rows, mark as processed and skip
                                if len(id_ref_map) < 1:
                                    handle_complete(connector, gse, gpl)
                                else:
                                    f = t_exec.submit(gse_gpl_processor.handle_gse_gpl, connector, ftp_handler, gse, gpl, id_ref_map, db_retry, ftp_retry, batch_size)
                                    f.add_done_callback(cb(connector, gse, gpl))
                            
                    data = comm.sendrecv(rank, dest = distributor_rank)
                print("Rank %d received terminator. Exiting data handler..." % rank)
    except Exception as e:
        print("An error has occured in rank %d while handling data: %s" % (rank, e), file = stderr)
        print("Rank %d encountered an error. Exiting data handler..." % rank)
        #notify the distributor that one of the ranks failed and will not be requesting more data by sending -1
        comm.send(-1, dest = distributor_rank)


############################################


################### main ###################


#master rank, run distributor
if rank == distributor_rank:
    #start data distribution
    distribute()
else:
    #handle data sent by distributor
    handle_data()

############################################
    













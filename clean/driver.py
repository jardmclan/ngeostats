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
# import time

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
processor_name = MPI.Get_processor_name()
table_name = "gsm_gene_vals_%d" % rank

############################################


############ helper functs #################

#note can't name a column "values"
#note default char encoding latin1, which is 1 byte per char (and will be sufficient for these fields)
#values field may have many values, varchar not sufficient for storage
# def create_gsm_val_table(connector):
#     query = text("""CREATE TABLE IF NOT EXISTS gsm_gene_vals (
#         gsm varchar(255) NOT NULL,
#         gene_id varchar(255) NOT NULL,  
#         expression_values MEDIUMTEXT NOT NULL,
#         PRIMARY KEY (gene_id, gsm)
#     );""")
#     connector.engine_exec(query, None, 0)


#use separate tables for each rank to prevent bottlenecking
def create_ranked_gsm_val_table(connector):
    query = text("""CREATE TABLE IF NOT EXISTS %s (
        gsm varchar(255) NOT NULL,
        gene_id varchar(255) NOT NULL,  
        expression_values MEDIUMTEXT NOT NULL,
        PRIMARY KEY (gene_id, gsm)
    );""" % table_name)
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
    query = text("SELECT ref_id, gene_id FROM gene_gpl_ref WHERE gpl = :gpl")
    params = {
        "gpl": gpl
    }
    res = connector.engine_exec(query, params, retry)
    #want to create id mapping for handle_gse_gpl method which is row_id to gene_id map
    id_ref_map = {}
    for row in res:
        ref_id = row[0]
        gene_id = row[1]
        #some non-gene_ids got through, filter by length (longest gene ids should be 9 digits) and check if all characters are numbers (valid gene ids should consist of only numbers)
        #if gene id invalid just skip
        if len(gene_id) < 10 and gene_id.isdigit():
            id_ref_map[ref_id] = gene_id
    return id_ref_map

############################################


########### main handlers ##################


def distribute():
    #distributor rank is not used for processing (only n - 1 processing ranks)
    ranks = comm.Get_size() - 1
   
    ################# config #####################

    db_retry = config["general"]["db_retry"]
    chunk_size = config["general"]["chunk_size"]
    db_config = config["extern_db_config"]

    #############################################

    data = None
    with DBConnector(db_config) as connector:
        data = get_gse_gpls(connector, db_retry)
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
    # #send terminator to db_op_rank
    # comm.send(None, dest = db_op_rank)
    #if every processor requested data and received terminator, all done
    print("Complete!")

        
# def db_ops():
#     ############# helper functs ##################

#     def submit_db_batch(connector, batch, retry):
#         if len(batch) > 0:
#             query = text("INSERT IGNORE INTO gsm_gene_vals (gsm, gene_id, expression_values) VALUES (:gsm, :gene_id, :values)")
#             connector.engine_exec(query, batch, retry)

#     ##############################################

#     ################# config #####################

#     db_config = config["extern_db_config"]
#     db_retry = config["general"]["db_retry"]

#     ##############################################

#     with DBConnector(db_config) as connector:
#         data = comm.recv()
#         while data is not None:
#             recv_rank = data[0]
#             batch = data[1]
#             #print("received data from rank: %d, length: %d" % (recv_rank, len(batch)))
#             success = True
#             try:
#                 submit_db_batch(connector, batch, db_retry)
#             except Exception as e:
#                 success = False
#                 print("An error occured while inserting database entries: %s" % e, file = stderr)
#             comm.send(success, dest = recv_rank)
#             data = comm.recv()
#     print("DB op handler received terminator. Exiting...")

    

def handle_data():

    ############# helper functs ##################

    # def submit_db_batch(connector, batch, retry):
    #     if len(batch) > 0:
    #         query = text("INSERT IGNORE INTO %s (gsm, gene_id, expression_values) VALUES (:gsm, :gene_id, :values)" % table_name)
    #         connector.engine_exec(query, batch, retry)

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
    
    def cb(connector, gse, gpl, batch_size):
        def _cb(f):
            e = f.exception()
            #print exceptions that occured during processing
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
                #create table for this rank to use if doesn't exist
                create_ranked_gsm_val_table(connector)
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
                                    f = t_exec.submit(gse_gpl_processor.handle_gse_gpl, connector, batch_size, table_name, ftp_handler, gse, gpl, id_ref_map, db_retry, ftp_retry)
                                    f.add_done_callback(cb(connector, gse, gpl, batch_size))
                    #check for critical errors that caused db connector or ftp handler to die and throw exception (can't do anything if those are dead)
                    if connector.disposed or ftp_handler.is_disposed():
                        raise Exception("A resource handler has been disposed due to an error.")
                    data = comm.sendrecv(rank, dest = distributor_rank)
                print("Rank %d received terminator. Exiting data handler..." % rank)
    except Exception as e:
        print("A critical error has occured in rank %d while handling data: %s" % (rank, e), file = stderr)
        print("Rank %d encountered an error. Exiting data handler..." % rank)
        #notify the distributor that one of the ranks failed and will not be requesting more data by sending -1
        comm.send(-1, dest = distributor_rank)


############################################


################### main ###################


#master rank, run distributor
if rank == distributor_rank:
    print("Starting distributor, rank: %d, node: %s" % (rank, processor_name))
    #start data distribution
    distribute()
# elif rank == db_op_rank:
#     print("Starting db op handler, rank: %d, node: %s" % (rank, processor_name))
#     db_ops()
else:
    print("Starting data handler, rank: %d, node: %s" % (rank, processor_name))
    #handle data sent by distributor
    handle_data()

############################################
    













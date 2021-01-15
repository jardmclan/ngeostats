#note a series can have multiple platforms (constituent samples have different platforms)
#note that while a sample can potentially be in multiple series the value should always be the same for a given sample and ref_id, so some will be overwritten with the same value


from concurrent.futures import ThreadPoolExecutor
from db_connect import DBConnector, DBConnectorError
from ftp_handler import FTPHandler
import gse_gpl_processor
from sys import argv, stderr
from sqlalchemy import text
from json import load
# import time


############## statics #####################


#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = load(f)

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
        ref_id = row[0]
        gene_id = row[1]
        #some non-gene_ids got through, filter by length (longest gene ids should be 9 digits) and check if all characters are numbers (valid gene ids should consist of only numbers)
        #if gene id invalid just skip
        if len(gene_id) < 10 and gene_id.isdigit():
            id_ref_map[ref_id] = gene_id
            print(gene_id)
    return id_ref_map

############################################


########### main handlers ##################




    

def handle_gse_gpl(gse, gpl):

    ############# helper functs ##################

    def submit_db_batch(connector, batch, retry):
        if len(batch) > 0:
            query = text("REPLACE INTO gsm_gene_vals (gsm, gene_id, expression_values) VALUES (:gsm, :gene_id, :values)")
            connector.engine_exec(query, batch, retry)

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
                #set of fields for database
                data = f.result()
                success = True
                #break into batches and send to db
                start = 0
                # print(rank)
                # print("%d items returned" % len(data))
                # print(data[0])
                # t = time.time()
                while start < len(data):
                    end = start + batch_size
                    if end > len(data):
                        end = len(data)
                    batch = data[start:end]

                    try:
                        submit_db_batch(connector, batch, db_retry)
                    except Exception as e:
                        success = False
                        print("An error occured while inserting database entries: %s" % e, file = stderr)

                    #backup code, central db insertion
                    ##################################

                    # send_pack = [rank, chunk]

                    # #receive success/fail signal
                    # success = comm.sendrecv(send_pack, dest = db_op_rank)
                    # #stop if failed while trying
                    

                    ###################################

                    if not success:
                        break

                    start = end
                # tt = time.time() - t
                # print("completing: time %d" % tt)
                #if all batches inserted successfully mark as complete
                if success:
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
    with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
        with DBConnector(db_config) as connector:
            #process data and request more until terminator received from distributor
            #process data
            #get row mappings
            id_ref_map = None
            id_ref_map = get_gpl_id_ref_map(connector, gpl, db_retry)
            # #no rows, mark as processed and skip
            # if len(id_ref_map) < 1:
            #     print("no data")
            # else:
            #     data = gse_gpl_processor.handle_gse_gpl(connector, ftp_handler, gse, gpl, id_ref_map, db_retry, ftp_retry)
            #     print("complete, num entires: %d" % len(data))


############################################


################### main ###################

gse = "GSE12673"
gpl = "GPL7119"
handle_gse_gpl(gse, gpl)

############################################
    













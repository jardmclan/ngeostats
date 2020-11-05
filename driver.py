
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
from db_connect import DBConnector
import sqlite3
from mpi4py.futures import MPIPoolExecutor

class QueryParamGen():
    def __init__(self, source, nodata):
        self.source = source
        self.nodata = nodata

    def __next__(self):
        row = next(self.source)
        for i in range(len(row)):
            #some data seems to have weird trailing newlines
            row[i] = row[i].strip()
            if row[i] == self.nodata:
                row[i] = None
        return row

#translate gene names (synonyms) to main symbol (should include main symbol name translated to itself for simplicity)
def create_gene_syn_table(cur):
    query = """CREATE TABLE IF NOT EXISTS name2symbol (
        synonym TEXT NOT NULL PRIMARY KEY
        symbol TEXT NOT NULL
    );"""

    cur.execute(g2a_query)

def create_gene_stat_table(cur):

    #use the set of ids as primary key (gene by itself might map to multiples and some ids might be null)
    query = """CREATE TABLE IF NOT EXISTS genestats (
        gene_id TEXT NOT NULL
        gsm TEXT NOT NULL,
        log2rat number NOT NULL,
        nlog10p number NOT NULL,
        PRIMARY KEY (gene_id, gsm)
    );"""

    cur.execute(g2a_query)


p_executor.submit(getData, gpl, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock, translator)

p_max = 5
t_max = 5

api_base = "http://localhost:5000"
endpoints = {
    sym2info: "/api/v1/values/gene_info",
    gpl2gse: "/api/v1/values/gpl_gse",
    gse2vals: "/api/v1/values/gse_values"
}

def create_gse_val_table(connector):
    query = """CREATE TABLE IF NOT EXISTS gene_vals (
        gene_id TEXT NOT NULL,
        gpl TEXT NOT NULL,
        gse TEXT NOT NULL,
        gsm TEXT NOT NULL,
        ref_id TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (gene_id, gse, gsm, ref_id)
    );"""
    connector.engine_exec(query, None, 0)


# def get_stats_from_genes(genes):
#     p_pool = ProcessPoolExecutor(p_max)
#     for gene in genes:
#         p.submit(get_stats_from_gene, gene)


# def get_stats_from_gene(gene):
#     t_pool = ThreadPoolExecutor(t_max)
#     query = "%s%s" % (api_base, endpoints["sym2info"])
#     gene_info = requests.get()


def get_gene_row_ids(connector, gpl):
    query = text("SELECT ref_id, gene_id FROM gene_gpl_ref_new WHERE gpl = ':gpl'")
    res = connector.engine_exec(query, {"gpl": gpl}, 0)
    return res

def get_gses_from_gpl(connector, gpl):
    query = text("SELECT gse FROM gse_gpl WHERE gpl = ':gpl'")
    res = connector.engine_exec(query, {"gpl": gpl}, 0)
    return res

def get_gses_from_gpl(dbf, gpl):
    query = ("SELECT gse FROM gse_gpl WHERE gpl = ?")
    con = sqlite3.connect(dbf)
    res = con.execute(query, (gpl,))
    return res


def get_gpls_from_gse(connector, gse):
    query = text("SELECT DISTINCT gpl FROM gse_gpl WHERE gse = ':gse'")
    res = connector.engine_exec(query, {"gse": gse}, 0)
    return res

#!!!A SERIES CAN HAVE MULTIPLE PLATFORMS (CONSTITUENT SAMPLES HAVE DIFFERENT PLATFORMS)
#need to start with series, get set of all series, for each series get platforms, then get ids

#note that while a sample can potentially be in multiple series the value should always be the same for a given sample and ref_id, so some will be overwritten with the same value
#AND (gsm, ref_id) is the minimal unique key


def main():
    #vars, from config
    dbf = ""
    ftp_retry = 5

    with DBConnector() as connector:
        gses = get_gses(connector)
        gse = gses.fetchone()
        while gse:
            ######
            #split this into mpi process
            ######
            gpls = get_gpls_from_gse(connector, gse)
            gpl = gpls.fetchone()
            ids = {}
            while gpl:
                gene_row_ids = get_gene_row_ids(connector, gpl)
                for ids in gene_row_ids:
                    row_id = ids[0]
                    gene_id = ids[1]
                    #map info to row id since the row id is going to be the main id you need to get the series info (everything else can be referenced by tis, shouldn't need until adding to db)
                    #just map the gene_id
                    ids[row_id] = gene_id
                gpl = gpls.fetchone()
            
                handle_gse_gpl(connector, gse, gpl, ids)
            gse = gses.fetchone()


            


        #get list of unique platforms from translation table
        #for each platform get series from metadata db; get (gene_id, row_id)s from translation table
        #get series values for rows with row_ids in set of (gene_id, row_id)s



if __name__ == "__main__":
    main()











def submit_db_batch(connector, batch, retry):
    if len(batch) > 0:
        query = text("REPLACE INTO gene_vals (gene_id, gpl, gse, gsm, ref_id, value) VALUES (:gene_id, :gpl, :gse, :gsm, :ref_id, :value)")
        connector.engine_exec(query, batch, retry)



#just store raw values in case want to do more with them later (apply sample control analysis, etc)
#table needs: (gene_id, gpl, gse, gsm, ref_id, value)
#note there may be multiple values of for the same gene for a given sample (accession series etc), these should typically be averaged out, provide ref_id to differentiate and provide primary key, and also provide back reference if needed for future analysis
# primary key (gene_id, gse, gsm, ref_id)
# note gsm can be in multiple gses, one gpl per gsm, gse can cover multiple gpls (assume this means that gpls must have same set of rows)
#note can use group by and avg to aggregate values in queries (e.g group by gsm and take average value for given gene_id)

#maybe also second table with pre-computed info for fast access, log_2(ratio), -log_10(p) 
#table, (gene_id, gse, gsm, log_2_rat, neg_log_10_p)

#change return from boolean to (boolean include, boolean continue)
def handle_gse_gpl(connector, gse, gpl, ids, ftp_handler, db_retry, ftp_retry, batch_size):
    #map gene ids to arrays of values
    gene_val_map = {}

    #ids (row_id, gene_id)[]
    row_ids = set(ids.keys())

    #preprocess ids
    row_gene_map = {}
    data = {}
    for id_set in ids:
        row_id = id_set[0]
        gene_id = id_set[1]

        row_gene_map[row_id] = gene_id

        data[gene_id] = {
            "gene_id": gene_id,
            "values": []
        }




    batch = []
    
    #(id_col, [translation_cols])
    header_info = None
    header = None

    values_map = []


    #super fast check
    def check_skip_continue(row):
        nonlocal row_ids
        nonlocal header
        if header is None:
            header = row
            #check that there are samples (first column is row ids)
            if len(row) < 2:
                return (False, False)
            #make gsms lowercase
            for i in range(1, len(header)):
                header[i] = header[i].lower()
                values_map.append({ids[row_id]: [] for row_id in ids})
            #don't add header to rows to send to handler, continue
            return (False, True)
        else:
            row_id = row[0]
            if row_id in row_ids:
                row_ids.remove(row_id)
                if len(row_ids) <= 0:
                    return (True, False)
            else:
                return (True, True)
        

    #create p value, 

    #map by gene_id

    

    #header is ID_REG, GSMXXX, ...
    def handle_row(row):
        nonlocal batch
        nonlocal header
        nonlocal ids
        nonlocal gene_val_map

        row_id = row[0]
        gene_id = ids[row_id]["gene_id"]
        gpl = ids[row_id]["gpl"]

        if 

        for i in range(1, len(row)):
            gsm = header[i]
            gsm_val = row[i]
            
            #minus one due to ref_id col offset
            values_map[i - 1] = 

            fields = {
                "gene_id": gene_id,
                "gpl": gpl,
                "gse": gse,
                "gsm": gsm,
                "ref_id": row_id,
                "value": gsm_val
            }
            batch.append(fields)
            if len(batch) % batch_size == 0:
                #just let errors be handled in thread executor callback, if a single batch fails just redo the whole platform for simplicity sake
                submit_db_batch(connector, batch, db_retry)
                batch = []
            submit_db_batch(connector, batch, db_retry)


    #let errors be handled by callee (log error and don't mark gpl as processed)
    #note that some of the unprocessed gpls may just not exist, can check those after
    try:
        ftp_handler.process_gse_data(gpl, check_skip_continue, handle_row, ftp_retry)
    #if a resource not found error was raised then the resource doesn't exist on the ftp server, just skip this one
    except ResourceNotFoundError:
        pass
    #submit anything leftover in the last batch
    submit_db_batch(connector, batch, db_retry)

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
from db_connect import DBConnector

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

def get_stats_from_genes(genes):
    p_pool = ProcessPoolExecutor(p_max)
    for gene in genes:
        p.submit(get_stats_from_gene, gene)


def get_stats_from_gene(gene):
    t_pool = ThreadPoolExecutor(t_max)
    query = "%s%s" % (api_base, endpoints["sym2info"])
    gene_info = requests.get()


def get_gene_row_ids(connector, gpl):
    query = text("SELECT ref_id, gene_id FROM gene_gpl_ref_new WHERE gpl = ':gpl'")
    connector.engine_exec(query, {"gpl": gpl}, 0)

def get_gses_from_gpl(connector, gpl):
    query = text("SELECT gse FROM gse_gpl WHERE gpl = ':gpl'")
    connector.engine_exec(query, {"gpl": gpl}, 0)

def get_gses(connector):
    pass

def get_gpls_from_gse(connector, gse):
    query = text("SELECT DISTINCT gpl FROM gse_gpl WHERE gse = ':gse'")
    connector.engine_exec(query, {"gse": gse}, 0)

#!!!A SERIES CAN HAVE MULTIPLE PLATFORMS (CONSTITUENT SAMPLES HAVE DIFFERENT PLATFORMS)
#need to start with series, get set of all series, for each series get platforms, then get ids

def main():
    with DBConnector() as connector:
        gses = get_gses(connector)
    for gse in gses:
        ######
        #split this into mpi process
        ######
        gpls = get_gpls_from_gse(connector, gse)
        ids = {}
        for gpl in gpls:
            for ids in get_gene_row_ids(connector, gpl):
                row_id = ids[0]
                gene_id = ids[1]
                #map info to row id since the row id is going to be the main id you need to get the series info (everything else can be referenced by tis, shouldn't need until adding to db)
                ids[row_id] = {
                    "gpl": gpl,
                    "gene_id": gene_id
                }
        
        handle_gse(connector, gse, ids)

            


        #get list of unique platforms from translation table
        #for each platform get series from metadata db; get (gene_id, row_id)s from translation table
        #get series values for rows with row_ids in set of (gene_id, row_id)s



if __name__ == "__main__":
    main()










#just store raw values in case want to do more with them later (apply sample control analysis, etc)
#table needs: (gene_id, gpl, gse, gsm, ref_id, value)
#note there may be multiple values of for the same gene for a given sample (accession series etc), these should typically be averaged out, provide ref_id to differentiate and provide primary key, and also provide back reference if needed for future analysis
# primary key (gene_id, gse, gsm, ref_id)
# note gsm can be in multiple gses, one gpl per gsm, gse can cover multiple gpls (assume this means that gpls must have same set of rows)
#note can use group by and avg to aggregate values in queries (e.g group by gsm and take average value for given gene_id)

#maybe also second table with pre-computed info for fast access, log_2(ratio), -log_10(p) 
#table, (gene_id, gse, log_2_rat, neg_log_10_p)

#change return from boolean to (boolean include, boolean continue)
def handle_gse(connector, gse, ids, ftp_handler, db_retry, ftp_retry, batch_size):
    #ids (row_id, gene_id)[]
    row_ids = set(ids.keys())

    #preprocess ids
    row_gene_map = {}
    data = {}
    for id_set of ids:
        row_id = id_set[0]
        gene_id = id_set[1]

        row_gene_map[row_id] = gene_id

        data[gene_id] = {
            "gene_id": gene_id
            "values": []
        }



    batch = []
    
    #(id_col, [translation_cols])
    header_info = None
    header = None

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
        

        

    #header is ID_REG, GSMXXX, ...
    def handle_row(row):
        nonlocal batch
        nonlocal header

        row_id = row[0]

        #check if 
        for i in range(1, len(row))
            gsm = header[i]
            gsm_val = row[i]



        gene_id = get_gene_id_from_row(header, row, header_info[1], g2a_db)
        ref_id = row[header_info[0]]
        #check if a gene_id was found
        if gene_id is not None:
            #process row into field ref dict
            fields = {
                "gpl": gpl,
                "ref_id": ref_id,
                "gene_id": gene_id
            }
            batch.append(fields)
            if len(batch) % batch_size == 0:
                #just let errors be handled in thread executor callback, if a single batch fails just redo the whole platform for simplicity sake
                submit_db_batch(connector, batch, db_retry)
                batch = []


    #let errors be handled by callee (log error and don't mark gpl as processed)
    #note that some of the unprocessed gpls may just not exist, can check those after
    try:
        ftp_handler.process_gse_data(gpl, check_continue, handle_row, ftp_retry)
    #if a resource not found error was raised then the resource doesn't exist on the ftp server, just skip this one
    except ResourceNotFoundError:
        pass
    #submit anything leftover in the last batch
    submit_db_batch(connector, batch, db_retry)
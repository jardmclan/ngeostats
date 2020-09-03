
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests

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
        gene_symbol TEXT NOT NULL
        gsm TEXT NOT NULL,
        log2rat number NOT NULL,
        nlog10p number NOT NULL,
        PRIMARY KEY (gene_symbol, gsm)
    );"""

    cur.execute(g2a_query)


p_executor.submit(getData, gpl, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock, translator)

p_max = 5
t_max = 5

def get_stats_from_genes(genes):
    p_pool = ProcessPoolExecutor(p_max)
    for gene in genes:
        p.submit(get_stats_from_gene, gene)


def get_stats_from_gene(gene):
    t_pool = ThreadPoolExecutor(t_max)
    




def main():
    f



if __name__ == "__main__":
    main()
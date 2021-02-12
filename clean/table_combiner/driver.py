import math
from db_connect import DBConnector, DBConnectorError
from sys import argv, stderr
from json import load
from time import sleep

##########################################

import mpi4py
mpi4py.rc.recv_mprobe = False

from mpi4py import MPI

comm = MPI.COMM_WORLD


##########################################

#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = load(f)


##########################################

#process rank
rank = comm.Get_rank()
size = comm.Get_size()
processor_name = MPI.Get_processor_name()

##########################################

#combine table t2 into t1
def combine_tables(t1, t2):
    print("Rank %d: Combining tables %s and %s (combine to %s)" % (rank, t1, t2, t1))
    global connector
    global config
    retry = config["general"]["db_retry"]
    query = """
        INSERT IGNORE INTO %s
        SELECT *
        FROM %s
    """ % (t1, t2)
    connector.engine_exec(query, None, retry)
    query = "DROP TABLE %s" % t2
    connector.engine_exec(query, None, retry)
    return t1


def partition_tables(tables):
    tables_range = len(tables)
    tables_pivot = math.ceil(tables_range / 2.0)
    tables_lower = tables[:tables_pivot]
    tables_upper = tables[tables_pivot:]
    return (tables_lower, tables_upper)

def partition(ranks, tables):
    ranks_range = ranks[1] - ranks[0]
    ranks_pivot = ranks[0] + math.ceil(ranks_range / 2.0)
    ranks_lower = [ranks[0], ranks_pivot]
    ranks_upper = [ranks_pivot, ranks[1]]

    table_parts = partition_tables(tables)
    tables_lower = table_parts[0]
    tables_upper = table_parts[1]

    return [(ranks_lower, tables_lower), (ranks_upper, tables_upper)]


def handle_tables_local(tables):
    if len(tables) < 1:
        raise ValueError("Tables list has no items. This should never happen unless the initial list is empty.")
    #only one table, reached bottom of recursion, return single table
    if len(tables) == 1:
        return tables[0]
    
    parts = partition_tables(tables)

    #combine to the right for consistency
    t1 = handle_tables_local(parts[1])
    t2 = handle_tables_local(parts[0])


    combined = combine_tables(t1, t2)
    return combined



def handle_tables(data):
    ranks = data[0]
    tables = data[1]

    if len(tables) < 1:
        raise ValueError("Tables list has no items. This should never happen unless the initial list is empty.")
    #only one table, reached bottom of recursion, return single table
    if len(tables) == 1:
        #send None to any leftover ranks in range to prevent them from blocking forever, they won't be used
        for i in range(ranks[0], ranks[1]):
            comm.send(None, dest = i)
        return tables[0]
    
    parts = partition(ranks, tables)

    t1 = None
    t2 = None
    
    #distribute first group
    dist_ranks = parts[0][0]
    dist_tables = parts[0][1]

    dist_rank_range = dist_ranks[1] - dist_ranks[0]
    #if no ranks to distribute to then handle the rest locally
    if dist_rank_range == 0:
        #note using ceil so first group (dist group) always will have equal or greater the number of ranks, so if first group 0, second group wont have any either (can use local for both)
        #always merge to the right, use second part as t1
        t1 = handle_tables_local(parts[1][1])
        t2 = handle_tables_local(parts[0][1])
    else:
        #first rank in set of distributor ranks next rank to send to
        dist_rank = dist_ranks[0]
        #advance lowest rank by 1 to get remaining ranks
        dist_ranks[0] += 1
        #data to distribute (parent (current rank), ranks, tables)
        dist_data = (rank, dist_ranks, dist_tables)
        #send off first group to be processed by the next rank
        comm.send(dist_data, dest = dist_rank)
        #recursively handle second group
        t1 = handle_tables(parts[1])
        #get the table sent off to the next rank
        t2 = comm.recv(source = dist_rank)


    combined = combine_tables(t1, t2)
    return combined



#if too many ranks should have way to indicate to remaining ranks that theyre not needed to end
def get_tables():
    data = comm.recv(source = MPI.ANY_SOURCE)
    tables = None
    combined = None
    #if data is None then this is an extra rank that won't be used, just exit
    if data is not None:
        parent = data[0]
        ranks = data[1]
        tables = data[2]
        combined = handle_tables((ranks, tables))
        comm.send(combined, dest = parent)

    print("Rank %s finished (combined table: %s)" % (rank, combined))
        


db_config = config["extern_db_config"]
with DBConnector(db_config) as connector:
    if rank == 0:
        print("Starting root, rank: %d, node: %s" % (rank, processor_name))
        #start at rank 1 (next rank)
        ranks = [1, size]

        tables = ["gsm_gene_vals_%d" % i for i in range(1, 41)]
        tables.append("gsm_gene_vals")

        root_table = handle_tables((ranks, tables))
        print("Complete! Root table %s" % root_table)
        
    else:
        print("Starting rank: %d, node: %s" % (rank, processor_name))
        get_tables()
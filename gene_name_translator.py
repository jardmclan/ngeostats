import csv
from db_connect import DBConnector
from sqlalchemy.sql import text
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count
from threading import Semaphore, Thread, Lock
import math
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, exc
from time import sleep
import random
from sys import stderr
#from mpi4py.futures import MPIPoolExecutor
import time

Base = declarative_base()

num_entries_name = 0
num_entries_symbol = 0

num_entries_name_sub = 0
num_entries_symbol_sub = 0

def create_tables(connector):
    #want gene name to be stored case sensitive
    #note that binary char set is case sensitive but has to be converted to a specific charset for case conversion, non-bin charsets with binary collation are case sensitive but can be made case insensitive directly
    #utf8mb4 is up to 4 byte utf8, can store more chars
    query = text("""CREATE TABLE IF NOT EXISTS gene_name_translation_test3 (
        gene_name TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
        gene_id TEXT NOT NULL,
        tax_id TEXT NOT NULL,
        PRIMARY KEY (gene_name(255), gene_id(255))
    );""")

    connector.engine_exec(query, None, 0)

    query = text("""CREATE TABLE IF NOT EXISTS gene_id_to_symbol_test3 (
        gene_id TEXT NOT NULL,
        gene_symbol TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
        PRIMARY KEY (gene_id(255))
    );""")

    connector.engine_exec(query, None, 0)





#just use batch size = 10000
def submit_name_batch(connector, batch, retry):
    global num_entries_name_sub
    num_entries_name_sub  += len(batch)
    #do nothing if empty list
    if len(batch) > 0:
        query = text("REPLACE INTO gene_name_translation_test3 (gene_name, gene_id, tax_id) VALUES (:gene_name, :gene_id, :tax_id)")
        connector.engine_exec(query, batch, retry)

def submit_symbol_batch(connector, batch, retry):
    global num_entries_symbol_sub
    num_entries_symbol_sub += len(batch)
    #do nothing if empty list
    if len(batch) > 0:
        query = text("REPLACE INTO gene_id_to_symbol_test3 (gene_id, gene_symbol) VALUES (:gene_id, :gene_symbol)")
        connector.engine_exec(query, batch, retry)




        

    

#create separate table as gene_id to symbol reference

def main():
    s_time = time.time()
    threads = 14
    with DBConnector() as connector:
        with ThreadPoolExecutor(threads) as t_exec:
            retry_limit = 5
            symbol_batch = []
            name_batch = []
            batches = 0
            #prevent memory issues, pause if more than 50 waiting to be processed
            throttle = Semaphore(threads + 50)
            def handle_gene(gene_info):
                nonlocal symbol_batch
                nonlocal name_batch
                nonlocal throttle
                global num_entries_name
                global num_entries_symbol

                batch_size = 10000

                name_batch.append({
                    "gene_name": gene_info["gene_symbol"],
                    "gene_id": gene_info["gene_id"],
                    "tax_id": gene_info["tax_id"]
                })
                num_entries_name += 1
                for synonym in gene_info["synonyms"]:
                    name_batch.append({
                        "gene_name": synonym,
                        "gene_id": gene_info["gene_id"],
                        "tax_id": gene_info["tax_id"]
                    })
                    num_entries_name += 1
                symbol_batch.append({
                    "gene_id": gene_info["gene_id"],
                    "gene_symbol": gene_info["gene_symbol"]
                })
                num_entries_symbol += 1
                def cb(f):
                    nonlocal batches
                    nonlocal throttle
                    e = f.exception()
                    if e:
                        print(e, file = stderr)
                    batches += 1
                    print("Completed %d batches" % batches)
                    throttle.release()

                if len(name_batch) >= batch_size:
                    throttle.acquire()
                    f = t_exec.submit(submit_name_batch, connector, name_batch, retry_limit)
                    f.add_done_callback(cb)
                    name_batch = []
                if len(symbol_batch) >= batch_size:
                    throttle.acquire()
                    f = t_exec.submit(submit_symbol_batch, connector, symbol_batch, retry_limit)
                    f.add_done_callback(cb)
                    symbol_batch = []

            create_tables(connector)

            with open("gene_info") as f:
                reader = csv.reader(f, delimiter="\t")
                
                tax_index = 0
                id_index = 1
                sym_index = 2
                #bar delimited (with caveats)
                syns_index = 4
                
                header = True
                # i = 0
                for line in reader:
                    #skip header
                    if header:
                        header = False
                        continue

                    tax_id = line[tax_index]
                    gene_id = line[id_index]
                    symbol = line[sym_index]

                    #gene ids with no symbol should be labeled as "NEWENTRY", but check "-" too just in case
                    #if no symbol just skip
                    if symbol == "-" or symbol.upper() == "NEWENTRY":
                        continue

                    synonyms = line[syns_index]
                    if synonyms == "-":
                        synonyms = []
                    else:
                        synonyms = construct_synonym_list(synonyms)
                    
                    # names = [symbol, *synonyms]
                    
                    gene_info = {
                        "gene_symbol": symbol,
                        "synonyms": synonyms,
                        "gene_id": gene_id,
                        "tax_id": tax_id
                    }
                    # throttle.acquire()
                    handle_gene(gene_info)
                    # if i > 1000000:
                    #     break
                    # i += 1
            submit_name_batch(connector, name_batch, retry_limit)
            submit_symbol_batch(connector, symbol_batch, retry_limit)
        print("Complete!")
    print(num_entries_name, num_entries_symbol)
    print(num_entries_name_sub, num_entries_symbol_sub)
    print(time.time() - s_time)


def construct_synonym_list(synonyms):
    syn_list = []
    if verify_synonyms(synonyms):
        syn_list = set(synonyms.split("|"))
    return syn_list


def verify_synonyms(synonyms):
    #if the synonyms list starts with two apostrophes then assume its nonsense
    return not synonyms.startswith("''")


        
        
if __name__ == "__main__":
    main()
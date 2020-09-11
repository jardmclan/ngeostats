import csv
import db_connect
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

Base = declarative_base()

duplicates_lock = Lock()
error_table_lock = Lock()
error_log_lock = Lock()
console_lock = Lock()


class GNTrans(Base):
    __tablename__="gene_name_translation"
    gene_name = Column(String, primary_key=True)
    gene_id = Column(String, primary_key=True)
    tax_id = Column(String)

def create_table(engine):
    query = text("""CREATE TABLE IF NOT EXISTS gene_name_translation (
        gene_name TEXT NOT NULL,
        gene_id TEXT NOT NULL,
        tax_id TEXT NOT NULL,
        PRIMARY KEY (gene_name(255), gene_id(255))
    );""")

    with engine.begin() as con:
        con.execute(query)


#split array into n pieces within one element of size
def split_arr(arr, n):
    split = []
    length = len(arr)
    avg = length / n
    lower = math.floor(avg)
    upper = lower + 1

    #baseline all lower (n chunks of size lower), how many remain? each of the remaining elements adds one to a group (upper group)
    n_upper = length - (n * lower)
    #rest are lower
    n_lower = n - n_upper
    p = 0
    #generate groups
    for i in range(n_upper):
        split.append(arr[p:p + upper])
        p += upper

    for i in range(n_lower):
        split.append(arr[p:p + lower])
        p += lower


    return split

#split array into chunks of size n
def chunk_arr(arr, n):
    chunks = []
    pos = 0
    while pos + n < len(arr):
        chunks.append(arr[pos:pos + n])
        pos += n
    #remaining
    if len(arr) - pos > 0:
        chunks.append(arr[pos:len(arr)])
    
    return chunks



def submit_db_batch(engine, batch, retry, delay = 0):

    def handle_failure(e, failed_items):
        #log error and batch elements
        with error_log_lock:
            with open("errors.log", "a") as f:
                f.write("%s\n" % str(e))
        with error_table_lock:
            with open("errors.table", "a") as f:
                for fields in failed_items:
                    f.write("%s,%s,%s\n" % (fields["gene_name"], fields["gene_id"], fields["tax_id"]))

    #retry limit exceded, log and return
    if retry < 0:
        handle_failure(Exception("Retry limit exceeded"), batch)
    #do nothing if empty list
    elif len(batch) > 0:
        #limit of how many items to submit to single insert
        insert_limit = 10000
        #how many partitions to split batch into on duplicate handling
        partitions = 10
        #pause if a delay was set
        sleep(delay)
        #break batch into chunks of insert limit elements
        subbatches = chunk_arr(batch, insert_limit)

        for fields_list in subbatches:
            #keep the try block outside of the begin block, begin block has logic for cleaning up the transaction, then should propogate exception outward
            try:
                with engine.begin() as con:
                    con.execute(GNTrans.__table__.insert(), fields_list)
            #duplicate primary key (gene name, gene id)
            except exc.IntegrityError as e:
                #single item, isolated duplicate, log and skip
                if len(fields_list) == 1:
                    fields = fields_list[0]
                    with duplicates_lock:
                        with open("duplicates.log", "a") as f:
                            f.write("%s,%s,%s\n" % (fields["gene_name"], fields["gene_id"], fields["tax_id"]))
                #break into partitions parts and retry on each part until isolate duplicate entry
                else:
                    sublists = split_arr(fields_list, partitions)
                    for sublist in sublists:
                        submit_db_batch(engine, sublist, retry)
            except exc.OperationalError as e:
                #check if deadlock error (code 1213)
                if e.orig.args[0] == 1213:
                    backoff = 0
                    #if first failure backoff of 0.25-0.5 seconds
                    if delay == 0:
                        backoff = 0.25 + random.uniform(0, 0.25)
                    #otherwise 2-3x current backoff
                    else:
                        backoff = delay * 2 + random.uniform(0, delay)
                    #retry with one less retry remaining and current backoff
                    submit_db_batch(engine, fields_list, retry - 1, backoff)
                #something else went wrong, log exception and add to failures
                else:
                    handle_failure(e, fields_list)
            #catch anything else and count as failure
            except Exception as e:
                handle_failure(e, fields_list)




complete_batches = 0
        
def exec_batch(engine, batch, retry, throttle, t_exec):
    throttle.acquire(True)
    
    f = t_exec.submit(submit_db_batch, engine, batch, retry)

    def cb(f):
        global complete_batches
        #declare as nonlocal, not sure if can use release otherwise
        nonlocal throttle
        e = f.exception()
        with console_lock:
            if e is not None:
                print(e, file=stderr)
        complete_batches += 1
        with console_lock:
            print("Completed %d batches" % complete_batches)
        throttle.release()
    f.add_done_callback(cb)
    


def main():
    engine = db_connect.get_db_engine()
    #wrap everything in try finally to make sure engine cleaned up on error and prevent hanging
    try:
        create_table(engine)

        batch_size = 100000
        retry_limit = 5
        #threads = max(cpu_count() - 1, 1)
        #limit to 5 threads to limit deadlocks in db
        threads = 5

        #initialize logs
        with open("duplicates.log", "w") as f:
            f.write("gene_name,gene_id,tax_id\n")
        with open("errors.table", "w") as f:
            f.write("gene_name,gene_id,tax_id\n")
        with open("errors.log", "w") as f:
            f.write("")


        with open("gene_info") as f:
            reader = csv.reader(f, delimiter="\t")
            
            tax_index = 0
            id_index = 1
            sym_index = 2
            #bar delimited
            syns_index = 4
            
            #don't hold more than 10 extra batches at a time in memory, so block at 10 in queue (assumes 10 in queue after exec limit + 10 outbound)
            throttle = Semaphore(threads + 10)
            with ThreadPoolExecutor(threads) as t_exec:

                batch = []
                header = True
                for line in reader:
                    #skip header
                    if header:
                        header = False
                        continue
                    tax_id = line[tax_index]
                    gene_id = line[id_index]
                    symbol = line[sym_index]

                    #doesn't look like any of the symbols are blank, but just in case should skip record if it is
                    if symbol == "-":
                        continue

                    synonyms = line[syns_index]
                    if synonyms == "-":
                        synonyms = []
                    else:
                        synonyms = synonyms.split("|")
                    
                    names = [symbol, *synonyms]


                    #table format
                    # name (sym or syn), id, tax

                    for name in names:
                        fields = {
                            "gene_name": name,
                            "gene_id": gene_id,
                            "tax_id": tax_id
                        }
                        batch.append(fields)
                        if len(batch) % batch_size == 0:
                            exec_batch(engine, batch, retry_limit, throttle, t_exec)
                            batch = []
                #execute whatever is left over as a final batch
                exec_batch(engine, batch, retry_limit, throttle, t_exec)
            print("Complete!")
    except Exception as e:
        print(e, file=stderr)
    finally:
        db_connect.cleanup_db_engine()


        
        
if __name__ == "__main__":
    main()
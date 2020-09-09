import csv
import db_connect
from sqlalchemy.sql import text
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count
from threading import Semaphore, Thread, Lock
import math
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, exc

Base = declarative_base()

log_lock = Lock()

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



def submit_db_batch(engine, fields_list, retry):
    if retry < 0:
        raise Exception("Retry limit exceded")
    #do nothing if empty list
    if len(fields_list) == 0:
        return
    # def exec_submit_batch(fields_list, engine):
        # engine.execute(GNTrans.__table__.insert(), fields_list)
        # for fields in fields_list:
        #     con.execute(query, **fields)
    #engine = db_connect.get_db_engine()
    # try:
    partitions = 10
    integrity_err = False
    with engine.begin() as con:
        try:
            con.execute(GNTrans.__table__.insert(), fields_list)
        except exc.IntegrityError as e:
            integrity_err = True
        except exc.OperationalError as e:
            print(e)
            exit(1)
            #check if deadlock error (code 1213)
            if e.orig.args[0] == 1213:
                #retry with one less retry remaining
                submit_db_batch(engine, fields_list, retry - 1)
    if integrity_err:
        if len(fields_list) == 1:
            fields = fields_list[0]
            #single item duplicate, log and skip
            with log_lock:
                with open("duplicates.log", "a") as f:
                    f.write("%s,%s,%s\n" % (fields["gene_name"], fields["gene_id"], fields["tax_id"]))

            pass
        else:
            #break into partitions parts and retry on each part until isolate issue
            sublists = split_arr(fields_list, partitions)
            for sublist in sublists:
                submit_db_batch(sublist)


        # threads = cpu_count()
        # split = split_arr(fields_list, threads)
        # t_list = []
        # for fields in split:
        #     t = Thread(target=exec_submit_batch, args=(fields, engine,))
        #     t.start()
        #     t_list.append(t)
        # for t in t_list:
        #     t.join()
    # finally:
    #     db_connect.cleanup_db_engine()

        
def exec_batch(engine, batch, retry, throttle, print_interval, batch_num, t_exec):
    throttle.acquire(True)
    
    f = t_exec.submit(submit_db_batch, engine, batch, retry)

    def cb_wrapper_because_python_scoping_sucks(batch_num, batch_size, throttle):
        def cb(f):
            #declare as nonlocal, not sure if can use release otherwise
            nonlocal throttle
            #something went wrong, raise the exception from the processor
            if f.exception() is not None:
                raise f.exception()
            else:
                if batch_num % print_interval == 0:
                    print("Completed batch %d (batch size %d)" % (batch_num, len(batch)))
            throttle.release()
        return cb 
    f.add_done_callback(cb_wrapper_because_python_scoping_sucks(batch_num, len(batch), throttle))
    


def main():
    engine = db_connect.get_db_engine()
    #wrap everything in try finally to make sure engine cleaned up on error and prevent hanging
    try:
        create_table(engine)
 

        print_interval = 1
        batch_size = 100000
        retry_limit = 5

        #initialize duplicate log
        with open("duplicates.log", "w") as f:
            f.write("gene_name,gene_id,tax_id\n")



        # query = text("INSERT INTO gene_name_translation VALUES (:gene_name, :gene_id, :tax_id);")

        with open("gene_info") as f:
            reader = csv.reader(f, delimiter="\t")
            
            tax_index = 0
            id_index = 1
            sym_index = 2
            #bar delimited
            syns_index = 4
            threads = max(cpu_count() - 1, 1)
            #don't hold more than 10 batches at a time in memory, so block at 10 in queue (assumes 10 in queue after exec limit + 10 outbound)
            throttle = Semaphore(threads + 10)
            with ThreadPoolExecutor(threads) as t_exec:

                batch = []
                header = True
                batches = 0
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
                            exec_batch(engine, batch, retry_limit, throttle, print_interval, batches, t_exec)
                            batch = []
                            batches += 1
                #execute whatever is left over as a final batch
                exec_batch(engine, batch, retry_limit, throttle, print_interval, batches, t_exec)
            print("Complete!")
            
    finally:
        db_connect.cleanup_db_engine()


        
        
if __name__ == "__main__":
    main()
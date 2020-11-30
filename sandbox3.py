# test = ["a", "b"]
# d = {
#     test[0]: test[1]
# }
# print(d)

from db_connect_test import DBConnector
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from sys import stderr


def run_query(connector):
    query = text("SELECT count(*) FROM gse_gpl_processed")
    res = connector.engine_exec(query, None, 5)
    print(res.fetchall)

def main():
    threads = 10

    #saturate 10 times
    num_run = threads * 10

    with ThreadPoolExecutor(threads) as t_exec:
        with DBConnector() as connector:
            for i in range(num_run):
                f = t_exec.submit(run_query, connector)
                def cb(f):
                    e = f.exception()
                    if e:
                        print(e, file = stderr)
                f.add_done_callback(cb)

if __name__ == "__main__":
    main()
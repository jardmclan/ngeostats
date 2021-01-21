from concurrent.futures import ThreadPoolExecutor
from db_connect import DBConnector, DBConnectorError
from ftp_handler import FTPHandler
import gse_gpl_processor
from sys import argv, stderr
from sqlalchemy import text
from json import load
from threading import Thread, Semaphore


############## config #####################


#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = load(f)

############################################


def compress_table(connector, table_name, throttle):
    query = text("ALTER TABLE %s ROW_FORMAT=COMPRESSED;" % table_name)
    connector.engine_exec(query, None, 0)
    print("Completed %s" % table_name)
    throttle.release()


def main():
    threads = 3
    throttle = Semaphore(threads)
    with DBConnector(config["extern_db_config"]) as connector:
        for i in range(36, 40):
            table_name = "gsm_gene_vals_%d" % i
            throttle.acquire()
            t = Thread(target = compress_table, args = (connector, table_name, throttle,))
            t.start()





if __name__ == "__main__":
    main()
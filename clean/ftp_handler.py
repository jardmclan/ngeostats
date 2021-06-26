import csv
import io
import gzip
import sqlite3
import math
import ftplib
from ftp_manager import FTPManager
import ftp_downloader
import sys
import traceback
from time import sleep
import time

#csv.field_size_limit(sys.maxsize)
csv.field_size_limit(100000000)

class FTPHandlerError(Exception):
    pass

class FTPHandlerDisposedError(Exception):
    pass

class FTPHandler:
    def __init__(self, ftp_base, ftp_pool_size, ftp_opts):
        self.manager = FTPManager(ftp_base, ftp_pool_size, **ftp_opts)
        self.disposed = False

    def __parse_data_table_gz(self, file, table_start, table_end, check_include_continue):
        #store data table rows so can quickly transfer data, less prone to disconnect issues and overall faster due to multiple reconnects for long running transfers
        data_rows = []
        with gzip.open(file, "rt", encoding = "utf8", errors = "ignore") as f:
            #read past header data to table
            for line in f:
                line = line.strip()
                if line == table_start:
                    break
            #quote none should prevent fields with single quote character from accidentally being overrun
            reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                #apparently some might have extra lines? just ignore
                if len(row) == 0:
                    continue
                if row[0] == table_end:
                    break
                include_continue = check_include_continue(row)
                if include_continue[0]:
                    data_rows.append(row)
                if not include_continue[1]:
                    break
        return data_rows
            


    def __stream_processor(self, table_start, table_end, check_include_continue):
        def stream_processor(file):
            return self.__parse_data_table_gz(file, table_start, table_end, check_include_continue)
        return stream_processor

    #shouldn't need a delay on retry, just getting another connection from the pool
    def process_gpl_data(self, gpl, check_include_continue, row_handler, retry):
        if self.is_disposed():
            raise FTPHandlerDisposedError("process_gpl_data called after disposed")
        table_start = "!platform_table_begin"
        table_end = "!platform_table_end"
        def get_data(ftp_con):
            return ftp_downloader.get_gpl_data_stream(ftp_con, gpl, self.__stream_processor(table_start, table_end, check_include_continue))
        data = self.__retrieve_data_r(get_data, check_include_continue, row_handler, retry)
        self.__handle_data_r(data, row_handler, retry) 

    
    def process_gse_data(self, gse, gpl, check_include_continue, row_handler, retry):
        if self.is_disposed():
            raise FTPHandlerDisposedError("process_gse_data called after disposed")
        table_start = "!series_matrix_table_begin"
        table_end = "!series_matrix_table_end"
        def get_data(ftp_con):
            return ftp_downloader.get_gse_data_stream(ftp_con, gse, gpl, self.__stream_processor(table_start, table_end, check_include_continue))
        
        retriever_time_start = time.time()
        data = self.__retrieve_data_r(get_data, check_include_continue, row_handler, retry)
        retriever_time_end = time.time()
        retriever_elapsed = retriever_time_end - retriever_time_start
        print("gse: %s, gpl: %s retrieved %d rows in %f seconds" % (gse, gpl, len(data), retriever_elapsed))
        
        processor_time_start = time.time()
        self.__handle_data_r(data, row_handler, retry)        
        processor_time_end = time.time()
        processor_elapsed = processor_time_end - processor_time_start
        print("gse: %s, gpl: %s processed %d rows in %f seconds" % (gse, gpl, len(data), processor_elapsed))
        

    def __retrieve_data_r(self, get_data, check_include_continue, row_handler, retry, ftp_con = None, last_error = None):
        if retry < 0:
            if ftp_con is not None:
                #release connection, there may be an issue with it, but it's not my problem anymore (should be picked up by heartbeat or something)
                self.manager.return_con(ftp_con)
            #raise retry limit exceeded error
            raise RuntimeError("A connection error has occured. Could not get FTP data. Last error: %s" % str(last_error))

        #if no connection provided get a new one
        if ftp_con is None:
            ftp_con = self.manager.get_con()
        #otherwise reconnect provided connection (failed in last iter)
        else:
            ftp_con = self.manager.reconnect(ftp_con)
        data = []
        try:
            data = get_data(ftp_con)
            self.manager.return_con(ftp_con)
        #problem with connection
        #this syntax though... ftplib.all_errors is a tuple of exceptions, have to add a second tuple containing extra exceptions to add exception (, at end of tuple forces type to tuple)
        except ftplib.all_errors + (ftp_downloader.FTPStreamException,) as e:
            #retry
            #info = sys.exc_info()
            data = self.__retrieve_data_r(get_data, check_include_continue, row_handler, retry - 1, ftp_con, e)
        #probably an issue with resource info or resource does not exist
        #shouldn't actually be a problem with the connection, assumes error was not in return_con call
        except Exception as e:
            self.manager.return_con(ftp_con)
            raise e
        return data

    def __handle_data_r(self, data, row_handler, retry, last_error = None):
        if retry < 0:
            #raise retry limit exceeded error
            raise RuntimeError("An error has occured while processing data. Last error: %s" % str(last_error))
        try:
            for row in data:
                row_handler(row)
        except Exception as e:
            self.__handle_data_r(data, row_handler, retry - 1, e)

    
    def dispose(self):
        if not self.disposed:
            self.manager.dispose()
            self.disposed = True
        
    def is_disposed(self):
        #something could cause manager to dispose, if it is then dispose self
        if self.manager.disposed:
            self.disposed = True
        return self.disposed

    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #print("Not an error: exiting ftp handler", file = sys.stderr)
        self.dispose()
        if exc_type is not None:
            raise FTPHandlerError("An error occured in the FTP Handler: type: %s, error: %s" % (exc_type.__name__, exc_val))



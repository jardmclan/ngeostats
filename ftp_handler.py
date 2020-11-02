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

class FTPHandlerError(Exception):
    pass

class FTPHandler:
    def __init__(self, ftp_base, ftp_pool_size, ftp_opts):
        self.manager = FTPManager(ftp_base, ftp_pool_size, **ftp_opts)



    def parse_data_table_gz(self, file, table_start, table_end, check_include_continue):
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
                if check_include_continue(row)[0]:
                    data_rows.append(row)
                if not check_include_continue[1]:
                    break
        return data_rows
            


    def stream_processor(self, table_start, table_end, check_include_continue):
        def _stream_processor(file):
            self.parse_data_table_gz(file, table_start, table_end, check_include_continue)
        return _stream_processor

    #shouldn't need a delay on retry, just getting another connection from the pool
    def process_gpl_data(self, gpl, check_include_continue, row_handler, retry):
        table_start = "!platform_table_begin"
        table_end = "!platform_table_end"
        def get_data(ftp_con):
            return ftp_downloader.get_gpl_data_stream(ftp_con, gpl, self.stream_processor(table_start, table_end, check_include_continue))
        self.__process_data_r(get_data, check_include_continue, row_handler, retry)

    
    def process_gse_data(self, gse, gpl, check_include_continue, row_handler, retry):
        table_start = "!series_matrix_table_begin"
        table_end = "!series_matrix_table_end"
        def get_data(ftp_con):
            return ftp_downloader.get_gse_data_stream(ftp_con, gse, gpl, self.stream_processor(table_start, table_end, check_include_continue))
        self.__process_data_r(get_data, check_include_continue, row_handler, retry)
        

    def __process_data_r(self, get_data, check_include_continue, row_handler, retry, ftp_con = None, last_error = None):
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
            data = get_data(ftp_con) #ftp_downloader.get_gse_data_stream(ftp_con, gse, gpl, self.stream_processor(table_start, table_end, check_include_continue))
            self.manager.return_con(ftp_con)
        #problem with connection
        #this syntax though... ftplib.all_errors is a tuple of exceptions, have to add a second tuple containing extra exceptions to add exception (, at end of tuple forces type to tuple)
        except ftplib.all_errors + (ftp_downloader.FTPStreamException,) as e:
            #retry
            #info = sys.exc_info()
            self.__process_data_r(gse, check_include_continue, row_handler, retry - 1, ftp_con, e)
        #probably an issue with resource info or resource does not exist
        #shouldn't actually be a problem with the connection, assumes error was not in return_con call
        except Exception as e:
            self.manager.return_con(ftp_con)
            raise e
        #else runs if no exception raised
        else:
            __handle_data_r(data, row_handler, retry)

    def __handle_data_r(data, row_handler, retry, last_error = None):
        if retry < 0:
            #raise retry limit exceeded error
            raise RuntimeError("An error has occured while processing data. Last error: %s" % str(last_error))
        try:
            for row in data:
                row_handler(row)
        except Exception as e:
            __handle_data_r(data, retry - 1, e)

    
    def dispose(self):
        self.manager.dispose()

    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        if exc_type is not None:
            raise FTPHandlerError("An error occured in the FTP Handler: type: %s, error: %s" % (exc_type.__name__, exc_val))



import ftplib
import threading
import sys

class ResourceNotFoundError(Exception):
    pass

#threadsafe
#consumes data as it goes to save memory
class CircularRWBuffer():
    def __init__(self, init_buffer_size):
        self.buf = bytearray(init_buffer_size)
        self.max_size = init_buffer_size
        self.size = 0
        self.head = 0
        self.tail = 0
        self.access_lock = threading.Lock()
        self.complete = False
        self.data_block = threading.Event()

    def write(self, data):
        write_size = len(data)
        # print(write_size)
        #a lot of things mess with state consistency, so just lock everything
        self.access_lock.acquire()
        #overflow, resize buffer and try again
        if self.size + write_size > self.max_size:
            self.__resize()
            #need to release lock before recursing
            self.access_lock.release()
            #try to write again with new larger buffer
            return self.write(data)

        #wrap around (note if head already wrapped cannot overlap tail without triggering overflow condition)
        elif self.head + write_size > self.max_size:
            len_to_end = self.max_size - self.head
            remainder = write_size - len_to_end

            self.buf[self.head : self.max_size] = data[0 : len_to_end]
            self.buf[0 : remainder] = data[len_to_end : write_size]
            self.head = remainder
        else:
            self.buf[self.head : self.head + write_size] = data
            #update head
            self.head = self.head + write_size

        self.size += write_size
        #indicate data is present
        self.data_block.set()

        self.access_lock.release()


        #return bytes written to buffer
        return write_size

    def end_of_data(self):
        self.access_lock.acquire()
        self.complete = True
        #set data block signal to make sure no read is stuck on completion
        self.data_block.set()
        self.access_lock.release()

    def get_size(self):
        size = 0
        with self.access_lock:
            size = self.size
        return size

    def read(self, read_size = None, block = True, timeout = None):

        #a lot of things mess with state consistency, so just lock everything
        self.access_lock.acquire()
        #block and wait for data if none present, block is true, and data not finished
        if block and not self.complete:
            #data block might be set if read waiting for more data, let me through if enough data for me
            #could lead to resource starving if one reader waiting on a lot of data, not an issue for this though (and can use timeout if need)
            if self.size == 0 or (read_size is not None and read_size > self.size):
                self.access_lock.release()
                if not self.data_block.wait(timeout):
                    raise TimeoutError("Read request timed out")
                self.access_lock.acquire()


        #data is finished and all read, return empty
        if self.complete and self.size == 0:
            self.access_lock.release()
            return bytes()
        
        #need to block until proper number of bytes ready!!! (issue is probably that read expects n bytes and providing less because it's ready)
        #only provide < asked for bytes if eof (data stream complete)
        #if not blocking then just read what's there
        if read_size is None or (read_size > self.size and (self.complete or not block)):
            read_size = self.size
        #set data block and retry read, should go through after more data written (note only can trigger if block is true, otherwise caught by if condition)
        elif read_size > self.size:
            self.data_block.clear()
            self.access_lock.release()
            return self.read(read_size, block, timeout)
        
        
        data = bytearray(read_size)
        #wrap around
        if self.tail + read_size > self.max_size:
            # print("read_wrapped")
            p1_len = self.max_size - self.tail
            remainder = read_size - p1_len
            data[0 : p1_len] = self.buf[self.tail : self.max_size]
            data[p1_len : read_size] = self.buf[0 : remainder]
            self.tail = remainder
        #not wrapped, can grab directly
        else:
            data[0 : read_size] = self.buf[self.tail : self.tail + read_size]
            self.tail = self.tail + read_size
        self.size -= read_size

        if self.size == 0:
            self.data_block.clear()

        self.access_lock.release()

        # print(self.tail)

        return bytes(data)


    def __resize(self):
        temp = self.buf
        temp_size = self.max_size
        self.max_size *= 2
        self.buf = bytearray(self.max_size)
        self.__transfer(temp, temp_size, self.buf)
        temp = None

    def __transfer(self, old, old_size, new):
        #wrapped around
        if self.head < self.tail:
            p1_len = old_size - self.tail
            new[0 : p1_len] = old[self.tail : old_size]
            new[p1_len : self.size] = old[0 : self.head]
        #not wrapped, can transfer directly
        else:
            new[0 : self.size] = old[self.tail : self.head]
        self.tail = 0
        self.head = self.size

    # #close the stream so 
    # def close():





type_details = {
    "platform": {
        "acc_prefix": "GPL",
        "resource_base": "/geo/platforms/",
        "resource_suffix": "soft/",
        "file_suffix": "_family.soft.gz"
    },
    "series": {
        "acc_prefix": "GSE",
        "resource_base": "/geo/series/",
        "resource_suffix": "matrix/",
        "file_suffix": "_series_matrix.txt.gz"
    }
}


# ftp = ftplib.FTP(ftp_base)
# ftp.login()



def get_ftp_files(con, dir):
    #may throw an error if something wrong with connection, catch in ftp handler
    files = con.ftp.nlst(dir)
    return files
    
def get_resource_dir(accession, resource_details):
    acc_prefix = resource_details["acc_prefix"]
    resource_base = resource_details["resource_base"]
    resource_suffix = resource_details["resource_suffix"]

    resource_id = accession[3:]
    id_len = len(resource_id)
    resource_prefix = ""
    if id_len > 3:
        resource_prefix = resource_id[:-3]
    resource_cat = "%s%snnn/%s/" % (acc_prefix, resource_prefix, accession)
    
    resource_dir = "%s%s%s" % (resource_base, resource_cat, resource_suffix)

    return resource_dir



def get_gpl_data_stream(con, gpl, stream_processor):
    resource_details = {
        "acc_prefix": "GPL",
        "resource_base": "/geo/platforms/",
        "resource_suffix": "soft/",
    }

    file_suffix = "_family.soft.gz"
    fname = "%s%s" % (gpl, file_suffix)
    resource_dir = get_resource_dir(gpl, resource_details)
    resource = "%s%s" % (resource_dir, fname)
    files = None
    #verify resource exists and thow exception if it doesn't
    try:
        files = get_ftp_files(con, resource_dir)
    #if temp error response should be resource not found
    except ftplib.error_temp as e:
        #raise a separate error if the issue was that the resource was not found (temp, 450), otherwise just reflect error
        if e.args[0][:3] == "450":
            raise ResourceNotFoundError("Resource dir not found %s" % resource_dir)
        else:
            raise e
    if resource not in files:
        raise ResourceNotFoundError("Resource not found %s" % resource)

    return get_data_stream_from_resource(con, resource, stream_processor)


def get_gse_data_stream(con, gse, gpl, stream_processor):
    resource_details = {
        "acc_prefix": "GSE",
        "resource_base": "/geo/series/",
        "resource_suffix": "matrix/",
    }
    #file name can be one or the other depending if associated with single platform or multiples
    file_suffix = "_series_matrix.txt.gz"

    resource = None
    resource_dir = get_resource_dir(gse, resource_details)
    
    file_single = "%s%s" % (gse, file_suffix)
    file_multiple = "%s-%s%s" % (gse, gpl, file_suffix)
    
    resource_single = "%s%s" % (resource_dir, file_single)
    resource_multiple = "%s%s" % (resource_dir, file_multiple)
    files = None
    try:
        files = get_ftp_files(con, resource_dir)
    #if temp error response should be resource not found
    except ftplib.error_temp as e:
        #raise a separate error if the issue was that the resource was not found (temp, 450), otherwise just reflect error
        if e.args[0][:3] == "450":
            raise ResourceNotFoundError("Resource dir not found %s" % resource_dir)
    if resource_single in files:
        resource = resource_single
    elif resource_multiple in files:
        resource = resource_multiple
    else:
        raise ResourceNotFoundError("Resource not found in dir %s" % resource_dir)
    return get_data_stream_from_resource(con, resource, stream_processor)

#series are <gse>_series_matrix.txt.gz if only one platform associated
#otherwise <gse>-<gpl>_series_matrix.txt.gz


class FTPStreamException(Exception):
    pass

#have to have internal error handling and reconnect, need to read file from start till done since compressed
class FTPDirectStreamReader():
    def __init__(self, con, resource, bufsize, chunksize = 2048, socket_retry_limit = 20):
        self.connected = False
        self.con = con
        self.resource = resource
        self.chunksize = chunksize
        self.bytes_read = 0
        self.socket_retries = socket_retry_limit
        self.sock = None
        #use intermediary buffer so can read consistent chunks (use the circularRWBuffer since it should be pretty efficient)
        self.buffer = CircularRWBuffer(bufsize)
        self.create_transfer_socket()

    def create_transfer_socket(self):
        self.con.ftp.voidcmd("TYPE I")
        #start RETR command on resource with offset of number of bytes read so far
        self.sock = self.con.ftp.transfercmd("RETR %s" % self.resource, rest = self.bytes_read)

    #should just return 0 bytes on failure
    def read(self, size):
        data = None
        read_data = None
        #read chunks to buffer until have enough data or until disposed due to error or end of stream
        while self.sock is not None and self.buffer.get_size() < size:
            retry = False
            got_data = True
            data = self.read_from_socket()
            #if data None there was an unrecoverable error getting data
            if data is not None:
                #track file position
                self.bytes_read += len(data)
                #reached end of stream
                if not data:
                    #check if something wrong with ftp connection and should retry data retreival (just continue loop)
                    #otherwise end of stream
                    if not self.check_retry_end_of_stream():
                        self.buffer.end_of_data()
                        break
                #write data to buffer
                self.buffer.write(data)
        #get requested bytes from buffer
        data = self.buffer.read(size)
        return data

    def read_from_socket(self):
        data = None
        #error on socket closed (will throw EOF file if data read fails in gzip)
        #if this throws an error (socket forcibly closed) is the entire connection dead, or can just make a new socket?
        try:
            data = self.sock.recv(self.chunksize)
        except OSError as e:
            connected = True
            #try to close socket and cleanup connection
            self.dispose()
            #try to reconnect if have retries remaining
            if self.socket_retries > 0:
                #less one socket retry
                self.socket_retries -= 1
                #try to send noop to see if FTP connection died completely
                try:
                    #if this successful just mark reconnected as False (was not reconnected, should proceed with )
                    self.con.ftp.voidcmd("NOOP")
                #connection to FTP server is dead, reconnect the connection, create a new transfer socket at the position last received from and retry read
                except Exception:
                    #reconnect to FTP server (threadless), if reconnect unsuccessful indicate not connected so don't try to create socket
                    if not self.con.reconnect(False):
                        connected = False
                #FTP connection not connected, leave data as None
                if connected:
                    try:
                        #create new transfer socket at position in file
                        self.create_transfer_socket()
                        #try to read from socket again
                        data = self.read_from_socket()
                    #if could not create socket then just leave data as None
                    except Exception:
                        pass
        return data

    #check if proper end of stream or due to ftp connection loss (verify ftp connection still active)
    def check_retry_end_of_stream(self):
        #return whether the connection was still active (if it was then should be at proper end of stream, otherwise should try to get data again)
        retry = True
        #dispose (close socket and cleanup connection) so can properly check source ftp connection
        self.dispose()
        try:
            #check if connection works
            self.con.ftp.voidcmd("NOOP")
            #connection still active, should be proper end of stream, no need retry
            retry = False
        #connection dead
        except Exception:
            #check if should retry connection
            if self.socket_retries > 0:
                # print(self.socket_retries)
                # print(self.bytes_read)
                #less one retry
                self.socket_retries -= 1
                #reconnect to FTP server (threadless), if reconnect unsuccessful just end data stream with no retry (FTP handler can use manager reconnect to get an available connection)
                if self.con.reconnect(False):
                    try:
                        #create new transfer socket at position in file
                        self.create_transfer_socket()
                    #could not create new transfer socket, don't retry
                    except Exception:
                        retry = false
                #could not reconnect, don't retry
                else:
                    retry = False
        return retry

    def dispose(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
            #if there was an issue with the connection this may error out
            try:
                #response will be a 4xx error response because transfer closed before complete, use getmultiline to get response with no error handling
                resp = self.con.ftp.getmultiline()
                #set ftp object's lastresp property to ensure object consistency
                self.con.ftp.lastresp = resp[:3]
            except:
                pass
        

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        if exc_type is not None:
            raise FTPStreamException("FTP stream reader failed with exception of type %s: %s" % (exc_type.__name__, exc_val))

        



def retr_data(con, resource, stream, blocksize, term_flag, t_data):
    try:
        con.ftp.voidcmd('TYPE I')
        with con.ftp.transfercmd("RETR %s" % resource) as sock:
            data = sock.recv(blocksize)
            while data:
                #check if should terminate
                if(term_flag.is_set()):
                    break
                stream.write(data)
                #get next block of data
                data = sock.recv(blocksize)
        #response will be a 4xx error response because transfer closed before complete, use getmultiline to get response with no error handling
        resp = con.ftp.getmultiline()
        #set ftp object's lastresp property to ensure object consistency
        con.ftp.lastresp = resp[:3]
        stream.end_of_data()
    except Exception as e:
        #set exception in thread data object to the thrown exception so can be handled by caller
        t_data["exception"] = e
        #mark stream end of data so data processor know's nothing else is coming
        stream.end_of_data()



def get_data_stream_from_resource(con, resource, stream_processor):
    # #256KB starting buffer
    # stream = CircularRWBuffer(262144)

    # term_flag = threading.Event()
    # t_data = {
    #     "exception": None
    # }
    # t = threading.Thread(target = retr_data, args = (ftp, resource, stream, 2048, term_flag, t_data,))
    # t.start()
    # err = None
    # try:
    #     data_processor(stream)
    # #store any exceptions and raise after cleanup
    # except Exception as e:
    #     err = e
    # #data processor finished, terminate read
    # term_flag.set()
    # #wait on transfer sock to shut down and resources to be released before returning to prevent conflicts
    # t.join()
    # #if there was an error in the ftp read thread then set the error to this
    # #prioratize throwing errors from ftp read thread (will overwrite error from data processor)
    # if t_data["exception"] is not None:
    #     err = t_data["exception"]
    # #raise any exceptions that were encountered in data handler
    # if err is not None:
    #     raise err
    data = None
    with FTPDirectStreamReader(con, resource, 8192) as stream:
        with con.op_lock:
            data = stream_processor(stream)
    return data
    


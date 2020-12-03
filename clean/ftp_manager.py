import time
from threading import Event, Lock
import ftplib
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from time import sleep
from threading import Thread


class FTPConnection:
    def __init__(self, uri, threaded = True):
        self.uri = uri
        self.connection_lock = Lock()
        self.op_lock = Lock()
        self.disposed = False
        self.heartbeat_f = None
        #flag initially false
        self.initialized = Event()
        self.__connect(threaded)
        

    def set_heartbeat_f(self, f):
        self.heartbeat_f = f
    
    def heartbeat_finished(self):
        if self.heartbeat_f is None:
            return True
        else:
            return self.heartbeat_f.done()

    def reconnect(self, threaded = True):
        self.initialized.clear()
        #try to disconnect current connection in case still connected
        self.__disconnect()
        return self.__connect(threaded)

    def dispose(self):
        #acquire connection lock to prevent conflicts if trying to connect
        with self.connection_lock:
            #no need to dispose if already disposed
            if not self.disposed:
                self.__dispose()

    def __dispose(self):
        self.disposed = True
        self.__disconnect()

    def is_initialized(self):
        return self.initialized.isSet()

    #returns if connection disposed (failed on init)
    def wait_init(self):
        #block on init
        self.initialized.wait()
        #return connection disposed
        return not self.disposed

    def __connect(self, threaded = True):
        success = True
        if threaded:
            self.__threaded_create_connection()
        else:
            self.__create_connection()
            success = not self.disposed
        return success

    def __threaded_create_connection(self):
        t = Thread(target = self.__create_connection)
        t.start()

    def __create_connection(self):
        with self.connection_lock:
            #if the connection is disposed don't connect
            if not self.disposed:
                try:
                    self.ftp = ftplib.FTP(self.uri, timeout = None)
                    self.ftp.login()
                except ftplib.all_errors as e:
                    self.__dispose()
            self.initialized.set()

    def __disconnect(self):
        try:
            self.ftp.quit()
        except:
            pass

    
class GetConnectionTimeoutError(Exception):
    pass

class FTPManager:
    #note heartbeat is in a separate thread, so takes pulse_threads + 1 threads
    def __init__(self, uri, size, heartrate = 2, pulse_threads = 1, startup_threads = 5, get_connection_timeout = 300):
        self.cons = Queue(size)
        self.all_cons = []
        self.all_cons_lock = Lock()
        self.disposed = False
        self.timeout = get_connection_timeout
        init_t_exec = ThreadPoolExecutor(startup_threads)
        for i in range(size):
            #use thread pool executor and threadless initialization to limit number of threads started up on initial connection
            init_t_exec.submit(self.__init_con, uri)
        t = Thread(target = self.__heartbeat, args = (heartrate, pulse_threads,))
        t.start()

    def __init_con(self, uri):
        con = FTPConnection(uri, False)
        self.cons.put(con)
        with self.all_cons_lock:
            #keep separate list tracking all connections for heartbeat
            self.all_cons.append(con)

    def __heartbeat(self, heartrate, threads):
        with ThreadPoolExecutor(threads) as t_exec:
            while not self.disposed:
                sleep(heartrate)
                with self.all_cons_lock:
                    for con in self.all_cons:
                        #check if the last heartbeat was finished, if it wasn't then don't submit another
                        if con.heartbeat_finished():
                            f = t_exec.submit(self.__check_pulse, con)
                            con.set_heartbeat_f(f)



    def __check_pulse(self, con):
        #if manager disposed just return
        if self.disposed:
            return
        #wait for initialization and check if connection failed
        if not con.wait_init():
            #note this acquires the all cons lock, should be fine since this is run in a thread (should be unlocked in caller)
            self.__connection_failed(con)
        #check connection
        try:
            #lock connection to eliminate conflicts
            with con.op_lock:
                con.ftp.voidcmd("NOOP")
        #reconnect on failure
        except ftplib.all_errors:
            con.reconnect()
        except Exception as e:
            print("Error in heartbeat thread: %s" % str(e), file = stderr)



    def get_con(self):
        if self.disposed:
            raise Exception("get_con called after disposed")
        #get connection from queue, block if none available
        try:
            con = self.cons.get(timeout = self.timeout)
        except Empty:
            raise GetConnectionTimeoutError("Timed out while attempting to get connection")
        #wait for connection to initialize
        #if the connection is disposed (failed to initialize) indicate failure and get next connection
        if not con.wait_init():
            self.__connection_failed(con)
            con = self.get_con()
        return con


    def return_con(self, con):
        if self.disposed:
            raise Exception("return_con called after disposed")
        self.cons.put(con)

    #connection failed while being used, try to reconnect or get another connection
    def reconnect(self, con):
        new_con = None
        if self.disposed:
            raise Exception("reconnect called after disposed")
        #connection is already being reinitialized, just wait
        if not con.is_initialized():
            #check if initialization was successful
            if con.wait_init():
                new_con = con
        #reconnect the connection (not threaded) and return after, if the connection failed to reconnect, try to get the next connection
        else:
            if con.reconnect(threaded = False):
                new_con = con
        if new_con is None:
            self.__connection_failed(con)
            new_con = self.get_con()
        return new_con

    #if the connection failed and was disposed remove from all connections list
    def __connection_failed(self, con):
        with self.all_cons_lock:
            try:
                self.all_cons.remove(con)
            #already removed
            except ValueError:
                pass
        #make sure connections list isn't empty, raise underflow error if it is
        if len(self.all_cons) < 1:
            #dispose, since can't really do anything
            self.dispose()
            raise Exception("Underflow error. Could not create any connections to FTP server")


    def dispose(self):
        #do nothing if already disposed
        if not self.disposed:
            with self.all_cons_lock:
                self.disposed = True
                for con in self.all_cons:
                    con.dispose()
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    




        




    








    
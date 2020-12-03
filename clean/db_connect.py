

import sqlalchemy
import json
from time import sleep
from sqlalchemy import exc
import random
from threading import Lock, Event

class DBConnectorError(Exception):
    pass

class DBConnector():
    def __init__(self, config = None):
        self.tunnel = None
        self.disposed = False
        self.exec_lock= Lock()
        self.exec_count_lock = Lock()
        self.exec_count = 0
        self.idle = Event()
        self.idle.set()
        #if the flag is not set then restarting
        self.started = Event()
        default_config_file = "config.json"

        if config is None:
            with open(default_config_file) as f:
                config = json.load(f)["extern_db_config"]

        self.config = config

        self.__start()
        self.started.set()
        

    def __start(self):
        if self.config["tunnel"]["use_tunnel"]:
            #only import if tunnel is None (no tunnel created before), note don't need to import this if tunneling not being used
            if self.tunnel is None:
                global SSHTunnelForwarder
                from sshtunnel import SSHTunnelForwarder
            self.__start_tunnel()
            
        self.__start_engine()

    def __start_tunnel(self):
        config = self.config
        tunnel_config = config["tunnel"]["tunnel_config"]
        self.tunnel = SSHTunnelForwarder(
            (tunnel_config["ssh"], tunnel_config["ssh_port"]),
            ssh_username = tunnel_config["user"],
            ssh_password = tunnel_config["password"],
            remote_bind_address = (tunnel_config["remote"], int(tunnel_config["remote_port"])),
            local_bind_address = (tunnel_config["local"], int(tunnel_config["local_port"])) if tunnel_config["local_port"] is not None else (tunnel_config["local"], )
        )
        #looks like this should solve hanging issue on stop
        self.tunnel.daemon_forward_servers = True
        self.tunnel.start()

    def __start_engine(self):
        config = self.config
        #create and populate sql configuration from config file
        sql_config = {}
        sql_config["lang"] = config["lang"]
        sql_config["connector"] = config["connector"]
        sql_config["password"] = config["password"]
        sql_config["db_name"] = config["db_name"]
        sql_config["user"] = config["user"]
        sql_config["port"] = config["port"] if self.tunnel is None else self.tunnel.local_bind_port
        sql_config["address"] = config["address"] if self.tunnel is None else self.tunnel.local_bind_host

        SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (sql_config["lang"], sql_config["connector"], sql_config["user"], sql_config["password"], sql_config["address"], sql_config["port"], sql_config["db_name"])

        #create engine from URI
        self.engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)
    

    def __restart_con(self):
        #don't want to restart multiple times
        if self.started.is_set():
            #clear the started flag until done restarting
            self.started.clear()
            def f():
                self.__dispose_engine()
                self.__start_engine()
                #restarting tunnel seems to cause issues sometimes
                # self.__cleanup_db_con()
                # self.__start()
            self.__block_exec_and_wait_idle(f)
            #done restarting, set started flag
            self.started.set()
        #if already restarting wait on restarted flag to be set
        else:
            self.started.wait()

    def __dispose_engine(self):
        try:
            self.engine.dispose()
        except:
            pass


    def __dispose_tunnel(self):
        if self.tunnel is not None:
            try:
                self.tunnel.stop()
            except:
                pass
    

    def __cleanup_db_con(self):
        if not self.disposed:
            self.__dispose_engine()
            self.__dispose_tunnel()

    def dispose(self):
        self.__cleanup_db_con()
        self.disposed = True



    # def get_engine(self):
    #     return self.engine

    def __block_exec_and_wait_idle(self, f):
        #stops new executions
        with self.exec_lock:
            #wait until idle
            self.idle.wait()
            #execute function that requires idle
            f()

    def __begin_exec(self):
        #acquire exec lock (block if waiting on restart)
        with self.exec_lock:
            #stop counter races
            with self.exec_count_lock:
                #connection not idle
                self.idle.clear()
                #add exec to counter
                self.exec_count += 1

    def __end_exec(self):
        #stop counter races
        with self.exec_count_lock:
            #remove exec from counter
            self.exec_count -= 1
            #if exec count 0 set idle
            if self.exec_count < 1:
                self.idle.set()


    def __engine_exec_r(self, query, params, retry, delay = 0, last_error = None):
        if retry < 0:
            raise Exception("Retry limit exceeded. Last error: %s" % last_error)
        #signal execution start
        self.__begin_exec()
        #with self.exec_lock:
        sleep(delay)
        res = None
        def get_backoff():
            backoff = 0
            #if first deadlock backoff of 0.15-0.3 seconds
            if delay == 0:
                backoff = 0.15 + random.uniform(0, 0.15)
            #otherwise 2-3x current backoff
            else:
                backoff = delay * 2 + random.uniform(0, delay)
            return backoff
        def restart_retry():
            nonlocal res
            #indicate end of execution to give space for retry (requires idle)
            self.__end_exec()
            #restart the connection
            self.__restart_con()
            #retry, will wait until connection restarted so shouldn't need backoff
            #backoff = get_backoff()
            res = self.__engine_exec_r(query, params, retry - 1, 0, last_error)
        restart = False

        #engine.begin() block has error handling logic, so try catch should be outside of this block
        #note caller should handle errors and cleanup engine as necessary (or use with)
        try:
            with self.engine.begin() as con:
                res = con.execute(query, params) if params is not None else con.execute(query)
        except exc.OperationalError as e:
            last_error = e
            #check if deadlock error (code 1213)
            if e.orig.args[0] == 1213:
                backoff = get_backoff()
                self.__end_exec()
                #retry with one less retry remaining and current backoff (no need to restart connection)
                res = self.__engine_exec_r(query, params, retry - 1, backoff, last_error)
            #something else went wrong, log exception and add to failures
            else:
                restart = True
        except Exception as e:
            last_error = e
            restart = True
        
        #restart connection if indicated
        if restart:
            #restart the connection and retry
            restart_retry()
        else:
            #execution ended
            self.__end_exec()
        
        #return query result
        return res


    def engine_exec(self, query, params, retry):
        if self.disposed:
            raise RuntimeError("Cannot call engine exec after cleanup_db_engine. Object disposed")
        res = self.__engine_exec_r(query, params, retry)
        return res

    

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        if exc_type is not None:
            raise DBConnectorError("An error occured while using database connector: type: %s, error: %s" % (exc_type.__name__, exc_val))


import sqlalchemy
import json
from time import sleep
from sqlalchemy import exc
import random

class DBConnectorError(Exception):
    pass

class DBConnector():
    def __init__(self, config = None):
        self.tunnel = None
        self.disposed = False

        default_config_file = "config.json"

        if config is None:
            with open(default_config_file) as f:
                config = json.load(f)["extern_db_config"]

        
        if config["tunnel"]["use_tunnel"]:    
            from sshtunnel import SSHTunnelForwarder

            tunnel_config = config["tunnel"]["tunnel_config"]
            self.tunnel = SSHTunnelForwarder(
                (tunnel_config["ssh"], tunnel_config["ssh_port"]),
                ssh_username = tunnel_config["user"],
                ssh_password = tunnel_config["password"],
                remote_bind_address = (tunnel_config["remote"], int(tunnel_config["remote_port"])),
                local_bind_address = (tunnel_config["local"], int(tunnel_config["local_port"])) if tunnel_config["local_port"] is not None else (tunnel_config["local"], )
            )

            self.tunnel.start()

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


    def get_engine(self):
        return self.engine

    def cleanup_db_engine(self):
        if not self.disposed:
            self.engine.dispose()
            if self.tunnel is not None:
                self.tunnel.stop()
            self.disposed = True


    def __engine_exec_r(self, query, params, retry, delay = 0):
        if retry < 0:
            raise Exception("Retry limit exceeded")
        #HANDLE THIS IN CALLER
        # #do nothing if empty list (note batch size limiting handled in caller in this case)
        # elif len(batch) > 0:
        sleep(delay)
        res = None
        #engine.begin() block has error handling logic, so try catch should be outside of this block
        #note caller should handle errors and cleanup engine as necessary
        try:
            with self.engine.begin() as con:
                res = con.execute(query, params) if params is not None else con.execute(query)
        except exc.OperationalError as e:
            #check if deadlock error (code 1213), or lost connection during query (code 2013)
            if e.orig.args[0] == 1213 or e.orig.args[0] == 2013:
                backoff = 0
                #if first failure backoff of 0.15-0.3 seconds
                if delay == 0:
                    backoff = 0.15 + random.uniform(0, 0.15)
                #otherwise 2-3x current backoff
                else:
                    backoff = delay * 2 + random.uniform(0, delay)
                #retry with one less retry remaining and current backoff
                res = self.__engine_exec_r(query, params, retry - 1, backoff)
            #something else went wrong, log exception and add to failures
            else:
                raise e
        #return query result
        return res


    def engine_exec(self, query, params, retry):
        if self.disposed:
            raise RuntimeError("Cannot call engine exec after cleanup_db_engine. Object disposed")
        return self.__engine_exec_r(query, params, retry)
    

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_db_engine()
        if exc_type is not None:
            raise DBConnectorError("An error occured while using database connector: type: %s, error: %s" % (exc_type.__name__, exc_val))
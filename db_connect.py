

import sqlalchemy
import json

config_file = "config.json"

engine = None
tunnel = None

def get_db_engine():

    global engine
    global tunnel

    if engine is not None:
        return engine

    config = None
    with open(config_file) as f:
        config = json.load(f)["extern_db_config"]

    
    if config["tunnel"]["use_tunnel"]:    
        from sshtunnel import SSHTunnelForwarder

        tunnel_config = config["tunnel"]["tunnel_config"]
        tunnel = SSHTunnelForwarder(
            (tunnel_config["ssh"], tunnel_config["ssh_port"]),
            ssh_username = tunnel_config["user"],
            ssh_password = tunnel_config["password"],
            remote_bind_address = (tunnel_config["remote"], int(tunnel_config["remote_port"])),
            local_bind_address = (tunnel_config["local"], int(tunnel_config["local_port"])) if tunnel_config["local_port"] is not None else (tunnel_config["local"], )
        )

        tunnel.start()

    #create and populate sql configuration from config file
    sql_config = {}
    sql_config["lang"] = config["lang"]
    sql_config["connector"] = config["connector"]
    sql_config["password"] = config["password"]
    sql_config["db_name"] = config["db_name"]
    sql_config["user"] = config["user"]
    sql_config["port"] = config["port"] if tunnel is None else tunnel.local_bind_port
    sql_config["address"] = config["address"] if tunnel is None else tunnel.local_bind_host

    SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (sql_config["lang"], sql_config["connector"], sql_config["user"], sql_config["password"], sql_config["address"], sql_config["port"], sql_config["db_name"])

    #create engine from URI
    engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)

    return engine


def cleanup_db_engine():

    global engine
    global tunnel

    if engine is not None:
        engine.dispose()
        engine = None
    if tunnel is not None:
        tunnel.stop()
        tunnel = None
from db_connect import DBConnector, DBConnectorError
from sys import argv, stderr
from json import load

#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = load(f)

db_config = config["extern_db_config"]
with DBConnector(db_config) as connector:
    # query = "LOCK TABLES gse_gpl_processed WRITE;"
    # connector.engine_exec(query, None, 0)
    query = "SELECT COUNT(*) FROM gse_gpl_processed WHERE processed = false;"
    r = connector.engine_exec(query, None, 0, 20).first()[0]
    print(r)
    #query = "UNLOCK TABLES;"
    connector.engine_exec(query, None, 0)
    
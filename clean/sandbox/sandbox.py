from db_connect import DBConnector, DBConnectorError
from ftp_handler import FTPHandler
import gse_gpl_processor

# gse = "GSE103605"
# gpl = "GPL13497"
gse = "GSE103560"
gpl = "GPL6096"

ftp_base = "ftp.ncbi.nlm.nih.gov"
ftp_pool_size = 10
ftp_opts = {
    "heartrate": 2,
    "pulse_threads": 2,
    "startup_threads": 4,
    "get_connection_timeout": 3600
}

db_config = {
    "lang": "mysql",
    "connector": "pymysql",
    "address": "172.31.228.2",
    "port": 3306,
    "db_name": "extern_ncbi_geo",
    "password": "mJ1H.aO]u*A)@tOBcU",
    "user": "ngeo_grant",
    "tunnel": {
        "use_tunnel": True,
        "tunnel_config": {
            "ssh": "172.31.100.10",
            "ssh_port": 22,
            "user": "ngeo",
            "password": "g3osp@m!",
            "local": "127.0.0.1",
            "local_port": None,
            "remote": "172.31.228.2",
            "remote_port": 3306
        }
    }
}



with FTPHandler(ftp_base, ftp_pool_size, ftp_opts) as ftp_handler:
    with DBConnector(db_config) as connector:
        gse_gpl_processor.handle_gse_gpl(connector, ftp_handler, gse, gpl, {}, 5, 5, 1000)
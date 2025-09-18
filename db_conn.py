import oracledb
import configparser

#application.properties values
config = configparser.ConfigParser()
config.read("application.properties", encoding="utf-8")
target_user = config['TARGET']['target.user']
target_password = config['TARGET']['target.password']
target_dsn = config['TARGET']['target.dsn']
oracle_client_location = config['COMMON']["oracle_client_location"]

oracledb.init_oracle_client(lib_dir=oracle_client_location)

# target DB connection pool
target_db = oracledb.create_pool(
    user=target_user,
    password=target_password,
    dsn=target_dsn,
    min=1,
    max=5,
    increment=1
)

print("Oracle DB1 connection pool created")

def get_pool(name) -> oracledb.ConnectionPool:
    """
    DB 풀 가져오기. 현재는 db1만 지원.
    나중에 db2 추가할 때 확장 가능.
    """
    if name == "target_db":
        return target_db
    raise ValueError(f"Unknown DB pool name: {name}")

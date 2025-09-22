import oracledb
import configparser

#application.properties values
config = configparser.ConfigParser()
config.read("application.properties", encoding="utf-8")

# target DB
target_user = config['TARGET']['target.user']
target_password = config['TARGET']['target.password']
target_dsn = config['TARGET']['target.dsn']
oracle_client_location = config['COMMON']["oracle_client_location"]

# log DB
log_user = config['LOG']['log.user']
log_password = config['LOG']['log.password']
log_dsn = config['LOG']['log.dsn']

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

# log DB connection pool
log_db = oracledb.create_pool(
    user=log_user,
    password=log_password,
    dsn=log_dsn,
    min=1,
    max=5,
    increment=1
)

print("Oracle connection pool created")

# DB 풀 매핑
POOLS = {
    "target_db": target_db,
    "log_db": log_db,
}

def get_pool(name: str) -> oracledb.ConnectionPool:
    """
    DB 풀 가져오기
    - target_db : 메인 타겟 DB
    - log_db    : 로그 저장용 DB
    """
    try:
        return POOLS[name]
    except KeyError:
        raise ValueError(f"Unknown DB pool name: {name}")

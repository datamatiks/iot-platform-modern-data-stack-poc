import duckdb

def get_db_conn(db=':memory:',is_shared=False):
    return duckdb.connect(database=db, read_only=is_shared)
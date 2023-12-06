import duckdb

def get_conn(db=':memory:',is_shared=False):
    return duckdb.connect(database=db, read_only=is_shared)



conn = get_conn('../../db/datahack.duckdb')

conn.sql('SELECT * FROM ingest_pipeline_audit_log').show()
metrics_df = conn.sql('SELECT * EXCLUDE(measured_day,measured_hour,measured_minute) FROM ingest_gauge_metrics').df()
metrics_df = metrics_df.set_index("measured_datetime")
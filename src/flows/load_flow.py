import db as db
import pandas as pd
import os

from duckdb import DuckDBPyConnection
from prefect import flow, task, get_run_logger,serve
from datetime import datetime, date
from pathlib import Path

####### HELPER FUNCTIONS ##########
def get_conn()->DuckDBPyConnection:
    return db.get_db_conn('db/datahack.duckdb')

def insert_to_audit_table(conn, record):
     query = f"""
        INSERT INTO ingest_pipeline_audit_log (
            ingest_filename, 
            last_datetime, 
            updated_by, 
            updated_date
        ) 
        VALUES {record}
     """
     return conn.execute(query)
 
def get_last_datetime(conn):
    last_datetime = conn.sql(
        """
        SELECT last_datetime 
        FROM ingest_pipeline_audit_log
        WHERE auditid = 
            ( SELECT max(auditid) 
            FROM ingest_pipeline_audit_log
            )
        """
        ).fetchone()[0]
    
    print(f'last_datetime:{last_datetime}')
    return last_datetime


####### TASKS FUNCTIONS ##########
@task
def display(conn):
    conn.sql('SELECT * FROM ingest_pipeline_audit_log').show()
    conn.sql('SELECT * FROM ingest_gauge_metrics').show()

@task
def process_files(conn, filepath: str):
    files =[ f for f in os.listdir(filepath) ]
    files_sorted = sorted(files)
    processed = []

    #Loop all through staged files
    for index, filename in enumerate(files_sorted):
        print(f"Processing --- {filename}" )

        filepath = Path("src/data/staging") / filename
        tmp_df = conn.sql(f"SELECT * FROM read_csv_auto('{filepath}',all_varchar=true)").df()
        tmp_df["measured_datetime"] = pd.to_datetime(tmp_df['Measured'])
        tmp_df["measured_day"] = pd.to_datetime(tmp_df['Measured']).dt.date
        tmp_df["measured_hour"] = pd.to_datetime(tmp_df['Measured']).dt.hour
        tmp_df["measured_minute"] = pd.to_datetime(tmp_df['Measured']).dt.minute
        tmp_df = tmp_df.drop(["Measured"], axis=1)
        
        tmp_df = tmp_df.replace(['-'], '0')
        sensors_tbl_stg = conn.sql("SELECT * FROM tmp_df")
        curr_last_datetime = get_last_datetime(conn)
        new_df = conn.sql(f"SELECT * FROM sensors_tbl_stg where measured_datetime > '{curr_last_datetime}'").df()

        # print('----new_df----START-----')
        # print(new_df)
        # print('----new_df----END-----')

        conn.execute('INSERT INTO ingest_gauge_metrics SELECT * FROM new_df')
        new_last_datetime = conn.sql('SELECT MAX(measured_datetime) FROM sensors_tbl_stg').fetchone()[0]

        audit_entry = (filename, new_last_datetime.strftime('%Y-%m-%dT%H:%M:%S'),'datamatiks',datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        insert_to_audit_table(conn, audit_entry)
        processed.append(filepath)

    return processed

@task
def move_to_processed(files):
    import shutil
    try:
        for fp in files:
            shutil.move(fp, "src/data/processed/")
            print(f'Moved file {fp} to processed - OK')
    except:
        raise Exception("Error in moving files")

####### FLOW FUNCTIONS ##########
@flow
def load_data_flow(name="Load data to duckdb flow ", log_prints=True):
    logger = get_run_logger()
    now = datetime.now()
    print(now)
    logger.info(f"Run date: {now}")

    conn = get_conn()
    filenames = process_files(conn, 'src/data/staging')
    move_to_processed(filenames)
    display(conn)

    conn.close()

if __name__ == "__main__":
    # load_data_deploy  = load_data_flow.to_deployment(name="load-data-deploy", interval=0)
    # #ingest_15_mins = ingestflow.ingest_gauge_data.to_deployment(name="ingest-15-mins", interval=900)
    # serve(load_data_deploy)
    load_data_flow()
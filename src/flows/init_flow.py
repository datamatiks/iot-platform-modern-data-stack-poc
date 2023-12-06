import os
import db as db
import pandas as pd


from duckdb import DuckDBPyConnection
from prefect import flow, task, get_run_logger,serve
from datetime import datetime, date
from pathlib import Path

####### HELPER FUNCTIONS ##########
def get_conn():
    return db.get_db_conn('db/datahack.duckdb')

def init_seq(seq_name):
   conn = get_conn()
   return conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name} START 1;")

def init_sensor_table():
    conn = get_conn()
    #create metadata look up table
    conn.execute(
        """
            DROP TABLE IF EXISTS gauge_sensors_metadata; 
            CREATE TABLE gauge_sensors_metadata AS 
            SELECT * 
            FROM read_csv_auto
            ('src/data/metadata/rainfall-and-stream-heights-metadata-20230917t110000.csv')
        """
    )
    
def init_audit_table():
    conn = get_conn()
    conn.execute(
        """
            DROP TABLE IF EXISTS ingest_pipeline_audit_log ;
            CREATE TABLE ingest_pipeline_audit_log
            (
                auditid integer primary key DEFAULT NEXTVAL('seq_auditid'),
                ingest_filename varchar(255) not null,
                last_datetime datetime not null,
                updated_by varchar(255),
                updated_date datetime
            )
        """
    )
   
def drop_table(tbl_name):
    conn = get_conn()
    return conn.execute(f"DROP TABLE IF EXISTS {tbl_name} ")

def drop_seq(seq_name):
    conn = get_conn()
    return conn.execute(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE")

def destroy():
    conn = get_conn()
    drop_table(conn, "ingest_gauge_metrics")
    drop_table(conn, "ingest_pipeline_audit_log")
    drop_seq(conn, "seq_auditid")

def insert_to_audit_table(record):
     conn = get_conn()
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
 
def get_last_datetime():
    conn = get_conn()
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
def init_tables():
    
    init_seq("seq_auditid")
    init_sensor_table()
    init_audit_table()


@task
def display():
    conn = get_conn()
    conn.sql('SELECT * FROM ingest_pipeline_audit_log').show()
    conn.sql('SELECT * FROM ingest_gauge_metrics').show()

@task
def process_seeds(filepath: str):
    conn = get_conn()
    files =[ f for f in os.listdir(filepath) ]
    files_sorted = sorted(files)

    #Loop all through seed files
    for index, filename in enumerate(files_sorted):
        print(f"Processing --- {filename}" )
        filepath = Path("src/data/seeds") / filename
        tmp_df = conn.sql(f"SELECT * FROM read_csv_auto('{filepath}',all_varchar=true)").df()
        tmp_df["measured_datetime"] = pd.to_datetime(tmp_df['Measured'])
        tmp_df["measured_day"] = pd.to_datetime(tmp_df['Measured']).dt.date
        tmp_df["measured_hour"] = pd.to_datetime(tmp_df['Measured']).dt.hour
        tmp_df["measured_minute"] = pd.to_datetime(tmp_df['Measured']).dt.minute
        tmp_df = tmp_df.drop(["Measured"], axis=1)
        
        tmp_df = tmp_df.replace(['-'], '0')
        
        #Create the table on first file
        if index == 0:
            
            drop_table('ingest_gauge_metrics')
            conn.execute("CREATE TABLE ingest_gauge_metrics AS SELECT * FROM tmp_df")

            last_datetime = conn.execute('SELECT MAX(measured_datetime) FROM ingest_gauge_metrics').fetchone()[0]

            print('Initial ingest')
            print(f'Processing {filename}')
            print(f'last_datetime : {last_datetime}') 
           
            audit_entry = (filename, last_datetime.strftime('%Y-%m-%dT%H:%M:%S'), 'datamatiks',datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
            insert_to_audit_table(audit_entry)
        else:
            sensors_tbl_stg = conn.sql("SELECT * FROM tmp_df")
            curr_last_datetime = get_last_datetime()
            new_df = conn.sql(f"SELECT * FROM sensors_tbl_stg where measured_datetime > '{curr_last_datetime}'").df()

            # print('----new_df----START-----')
            # print(new_df)
            # print('----new_df----END-----')

            conn.execute('INSERT INTO ingest_gauge_metrics SELECT * FROM new_df')
            new_last_datetime = conn.sql('SELECT MAX(measured_datetime) FROM sensors_tbl_stg').fetchone()[0]

            audit_entry = (filename, new_last_datetime.strftime('%Y-%m-%dT%H:%M:%S'),'datamatiks',datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
            insert_to_audit_table(audit_entry)

####### FLOW FUNCTIONS ##########
@flow
def init_db_flow(name="Initialize DB flow ", log_prints=True):
    logger = get_run_logger()
    now = datetime.now()
    print(now)
    logger.info("%s Run date🤓:", now)

    init_tables()
    process_seeds('src/data/seeds')
    display()

if __name__ == "__main__":
    init_db_flow()
    
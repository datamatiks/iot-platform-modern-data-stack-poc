# import db as db
# import httpx
# from datetime import datetime, date

# conn = db.get_db_conn()
# conn.sql('SELECT * FROM ingest_pipeline_audit_log').show()
# conn.sql('SELECT * FROM ingest_gauge_metrics').show()

from urllib.request import urlopen
import json
import pandas as pd
import httpx


try:
    # response = urlopen('http://0.0.0.0:8000/api/v1/metadata')
    # json_data = response.read().decode('utf-8', 'replace')
    # d = json.loads(json_data)

    # metadata_df = pd.json_normalize(d['Sensor ID'])
    # print(metadata_df)
    
    metadata = pd.read_csv('http://0.0.0.0:8000/api/v1/metadata',
        header=0, names=['sensor_id', 'location_id','location_name','sensor_type','unit','latitude','longitude'])
    print(metadata.head())
except Exception as ex:
        print(ex)
        raise Exception(f"Something happen: {ex}")
'''
df = pd.read_json('https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-ETC')
df = pd.DataFrame(df['result'].values.tolist())
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df = df.set_index('TimeStamp')
print (df.head())

'''
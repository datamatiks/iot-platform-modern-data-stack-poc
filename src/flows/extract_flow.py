import db as db
import httpx
from datetime import datetime, date

from prefect import flow, task, get_run_logger,serve
from pathlib import Path
from dotenv import load_dotenv

DOWNLOAD_URL = "https://www.data.brisbane.qld.gov.au/data/dataset/01af4647-dd69-4061-9c68-64fa43bfaac7/resource/78c37b45-ecb5-4a99-86b2-f7a514f0f447/download/"

"""
Extract latest file from Brisbane Open Data Portal
https://www.data.brisbane.qld.gov.au/data/dataset/telemetry-sensors-rainfall-and-stream-heights
"""
@task
def get_gauge_data() -> str:

    now = datetime.today()
    date_str = str(now.date()) 
    date_str = date_str.replace("-","")
    print(date_str)

    filename = f"gauge-data-{date_str}*.csv"
    print("filename:" + filename)
    try:
        r = httpx.get(f'{DOWNLOAD_URL}{filename}')
    
        now_str= str(now).replace(" ","-")
        outfilename = f'src/data/staging/new-file-{now_str}.csv'

        with open(outfilename, 'w') as f:
            f.write(r.text)
    
        return outfilename
    
    except Exception as ex:
        print(ex)
        raise Exception(f"Something happen: {ex}")


@flow
def extract_file_flow(name="Gauge Data Extract", log_prints=True):
    logger = get_run_logger()
    now = datetime.now()
    print(now)
    logger.info(f"Run date🤓: - {now} -")

    filename = get_gauge_data()
    logger.info(f"New file extracted: {filename}")
    #merge_gauge_data(filename)

if __name__ == "__main__":
    # extract_file_deploy_15mins  = extract_file_flow.to_deployment(name="extract-file-deploy-15mins", interval=900)
    # serve(extract_file_deploy_15mins)
    extract_file_flow()
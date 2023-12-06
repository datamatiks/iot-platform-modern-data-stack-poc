
from typing import Union
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import io

from  .data  import Database

app = FastAPI()
db = Database()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/api/v1/daily_avg", response_class=StreamingResponse)
def get_daily_avg():
    avg_df = db.get_daily_avg()
    avg_df = avg_df.reset_index()
    
    return _to_csv(avg_df,'daily_avg')

@app.get("/api/v1/daily_max", response_class=StreamingResponse)
def get_daily_max():
    max_df = db.get_daily_max()
    max_df = max_df.reset_index()
    
    return _to_csv(max_df,'daily_max')

@app.get("/api/v1/daily_min" ,response_class=StreamingResponse)
def get_daily_min():
    min_df = db.get_daily_min()
    min_df = min_df.reset_index()
    
    return _to_csv(min_df,'daily_min')


@app.get("/api/v1/metadata", response_class=StreamingResponse)
def get_metadata():
    mdf =  db.get_gauge_metadata()
    return _to_csv(mdf,'metadata')

@app.get("/api/v1/sensor_metrics", response_class=StreamingResponse)
def get_sensor_metrics():
    print("api/v1/sensor_metrics")
    gdf = db.get_gauge_metrics()
    gdf = gdf.reset_index()

    return _to_csv(gdf,'metrics')


def _to_csv(df, fname):
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(
        iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={fname}.csv"

    return response
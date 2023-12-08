import streamlit as st
import pandas as pd
import plost
import altair as alt
import math
import os

from dotenv import load_dotenv
from urllib.request import urlopen
import json
import folium
from streamlit_folium import st_folium, folium_static
from streamlit_plotly_events import plotly_events
import plotly.figure_factory as ff
import plotly.express as px
import numpy as np
from datetime import datetime

load_dotenv()

#API_URL = 'http://0.0.0.0:8000'
API_URL = 'http://iot-platform-modern-data-stack-poc-api-1:8000/api/v1'
st.set_page_config(layout='wide', initial_sidebar_state='expanded')

dt_now = datetime.now()
dt_day = dt_now.strftime('%A')
_,colT2 = st.columns([3,7])
with colT2:
    st.title('Brisbane Weatherboard')
    st.write(f"### {dt_day}, {dt_now}")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    
st.sidebar.header('Weatherboard `v.1.0`')

st.sidebar.subheader('Map parameter')
time_hist_color = st.sidebar.selectbox('Display by sensor', ('Rainfall', 'Stream')) 

st.sidebar.subheader('Heat map parameter')
time_hist_color = st.sidebar.selectbox('Color by', ('min_value', 'max_value')) 

#st.sidebar.subheader('Donut chart parameter')
#donut_theta = st.sidebar.selectbox('Select data', ('Active', 'Inactive'))

st.sidebar.subheader('Line chart parameters')
plot_data = st.sidebar.multiselect('Select data', ['avg_value', 'max_value','min_value'], ['avg_value', 'min_value','max_value'])
plot_height = st.sidebar.slider('Specify plot height', 0,2,1)

st.sidebar.markdown('''
---
Created with ❤️ by [Data Professor](https://youtube.com/dataprofessor/).\n\n
                    
Modified with passion by Datamatiks.
''')

# Metrics row 
st.markdown('### Weather Metrics')
col1, col2, col3 = st.columns(3)

#Retrive other weather data from public api
response = urlopen("https://wttr.in/Brisbane?format=j1")
json_data = response.read().decode('utf-8', 'replace')
d = json.loads(json_data)

df_brissy = pd.json_normalize(d['current_condition'])
temp = df_brissy['temp_C'].values[0]
wind = df_brissy['windspeedKmph'].values[0]
humid = df_brissy['humidity'].values[0]
col1.metric("Temperature", f"{temp} °C", "1.2 °C")
col2.metric("Wind", f"{wind} kph", "-8%")
col3.metric("Humidity", f"{humid}%", "4%")

#retrive metadata
metadata = pd.read_csv(
    f'{API_URL}/metadata',
    header=0, 
    names=['sensor_id', 
            'location_id',
            'location_name',
            'sensor_type',
            'unit',
            'latitude',
            'longitude']
)

#retrieve daily metrics
sensors_df = pd.read_csv(f'{API_URL}/sensor_metrics',header=0)
avg_df = pd.read_csv(f'{API_URL}/daily_avg',header=0)
max_df = pd.read_csv(f'{API_URL}/daily_max',header=0)
min_df = pd.read_csv(f'{API_URL}/daily_min',header=0)

new_avg_df = avg_df.melt( id_vars=["measured_datetime"], 
         var_name="sensor_id", 
         value_name="avg_value")

new_max_df = max_df.melt( id_vars=["measured_datetime"], 
         var_name="sensor_id", 
         value_name="max_value")

new_min_df = min_df.melt( id_vars=["measured_datetime"], 
         var_name="sensor_id", 
         value_name="min_value")

#merge all metrics
merge_keys=['sensor_id','measured_datetime']
daily_metrics_df = new_avg_df.merge(new_max_df, on=merge_keys).merge(new_min_df, on=merge_keys).set_index('sensor_id')

#enrich metrics with metadata
joined_df = daily_metrics_df.join(metadata.set_index('sensor_id')).reset_index()


date_now_str = str(dt_now.date()) 
df_filtered = joined_df.loc[(joined_df['measured_datetime'] == date_now_str)]
df_active = df_filtered[df_filtered['avg_value']>0].groupby('sensor_id').apply(len)
df_inactive = df_filtered[df_filtered['avg_value']==0].groupby('sensor_id').apply(len)
df_unknown = df_filtered[df_filtered['avg_value'].isnull() ].groupby('sensor_id').apply(len)

gauge_status_df = pd.DataFrame(
        {'status':['Active','Inactive','Unknown'],                              
         'count': [len(df_active.index),len(df_inactive.index),len(df_unknown.index)]
        }
)

### Map plotly version
st.markdown('### Rainfall and Stream AHD Height Sensors Map')
px.set_mapbox_access_token(os.environ['MAPBOX_TOKEN'] )
df = metadata.__deepcopy__()

df = joined_df.fillna(0)
fig = px.scatter_mapbox(df, lat="latitude", lon="longitude",  hover_name="location_name", hover_data=["sensor_type", "min_value"], color="avg_value", size="max_value",
                color_continuous_scale=px.colors.cyclical.IceFire, size_max=50, zoom=10, width=1500, height=800)
st.plotly_chart(fig)

#Heatmap
c1, c2 = st.columns((7,3))
with c1:
    st.markdown('### Heatmap')
    plost.time_hist(
    data=joined_df,
    date='measured_datetime',
    x_unit='hours',
    y_unit='day',
    color=time_hist_color,
    aggregate='median',
    legend=None,
    height=345,
    use_container_width=True)
with c2:
    st.markdown('### Sensors status')
    plost.donut_chart(
        data=gauge_status_df,
        theta='count',
        color='status',
        legend='bottom', 
        use_container_width=True)

# Row C
st.markdown('### Line chart')
grouped = joined_df.groupby('sensor_type')
for key, group in grouped:
    st.markdown(f"*** {key}")
    #st.write(group)
    df = group.set_index('measured_datetime')
    df = group.astype({'avg_value': 'float','max_value': 'float','min_value': 'float'})
    plost.line_chart(df, 
        x = 'measured_datetime' ,
        y = ('min_value','max_value','avg_value'),
        color='sensor_id',
        height=plot_height, 
        #pan_zoom='minimap'
    )

st.write(joined_df.reset_index().sort_values(by='measured_datetime', ascending=False))
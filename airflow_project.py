# import python modules
import http.client
import urllib.request
import urllib.parse
import urllib.error
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pandas_redshift as pr
import reverse_geocoder as rg
import os
import sys
from dateutil.parser import parse
#Airflow dependencies
import airflow
from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import Variable
from airflow.models import XCom
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

def convert_datetime(date: str):
    """
    - takes string datetime (RFC3339) object
    - convert to standard datetime
    return: datetime object
    """
    if type(date) is not str:
        return parse(str(date)).replace(tzinfo=None)
    return parse(date).replace(tzinfo=None)

def get_data(**kwargs):
    """
    - connect to WOW API and send query against the database
    - read the data and parsed into a dictionary object
    - push the raw data to airflow database in order to be access 
       by other airflow task
    """
    ti = kwargs['ti']
    # Variable.get() extract the actual key stored in airflow database.
    # This is for security purposes
    headers = {"Ocp-Apim-Subscription-Key": Variable.get("WOW_SUBSCRIPTION_KEY")}
    conn = http.client.HTTPSConnection('mowowprod.azure-api.net')
    conn.request("GET", "/api/observations/geojson", "{body}", headers)
    response = conn.getresponse()
    # check if the connection was successful before proceeding
    if (str(response.status) == "200"):
        print("Connection status:", response.status,response.reason)
        data = response.read()
        conn.close()
    else:
        raise ConnectionError("Connection FAILED:",response.status, response.reason)
    # decode and parse the to dictionary
    parse_data = json.loads(data)
    # push data to xcom
    ti.xcom_push(key="wow_data",value=parse_data)
    print(sys.getsizeof(parse_data),"bytes of data is pushed to xcom")

def extract_observations(**context):
    """
    - pull the raw data to extract features and values
    - serialize the data and normalize it into flat table
    - convert data frame into dictionary and push to the next task through xcoms
    """
    ti = context['ti']
    # pull the data
    data = ti.xcom_pull(key='wow_data')
    print(sys.getsizeof(data), "bytes of data is pulled from xcom")
    properties = []
    for item in data["features"]:
        properties.append(item["properties"])
    # serialize dictionary object to pandas dataframe
    df = pd.json_normalize(properties)
    # convert dataframe to dictiory
    # because xcoms only takes serializable object
    json_df = df.to_json(orient="records")
    #push dict object to xcom
    ti.xcom_push(key='extract_observations', value=json_df)
    print(sys.getsizeof(json_df), "bytes of data is pushed to xcom")


def transform_observations(**context):
    """
    - pulls the data from the previous task
    - convert it to pandas dataframe
    - rename table columns and drop unwanted columns
    - push processed table to the next task (loading data to data warehouse)
    """
    ti = context['ti']
    str_df = ti.xcom_pull(key='extract_observations')
    print(sys.getsizeof(str_df), "bytes of data is pulled from xcom")
    # parse data
    dict_df = json.loads(str_df)
    # normalize dictionary object to pandas dataframe
    df = pd.json_normalize(dict_df)
    if ('primary.dm' and 'primary.dap' in df.columns):
        df['primary.dm'] = df['primary.dm'].fillna(df['primary.dap'])
        df = df.drop(columns=['primary.dap'])
    #check if 'dap' and 'dm' in df
    if ('primary.dap' in df.columns):
        if ('primary.dm' not in df.columns):
          df = df.rename(columns={'primary.dap': 'primary.dm'})
    cols = ['reportId', 'siteId', 'primary.dt', 'primary.dpt',
            'primary.dm', 'primary.dh', 'primary.dwd', 'primary.dws']
    for col in df.columns:
        if col not in cols:
            df = df.drop(columns=[col])
    # rename dataframe columns
    df = df.rename(columns={'primary.dt': 'temperature', 'primary.dpt': 'dew_temperature',
                            'primary.dws': 'wind_speed', 'primary.dm': 'mean_sea_level',
                            'primary.dh': 'humidity', 'primary.dwd': 'wind_direction', })
    # roundoff the values and drop nulls
    df = df.round(1).dropna()
    # convert dataframe to dictionary
    json_df = df.to_json(orient="records")
    # push dictionary object to xcom
    ti.xcom_push(key='transform_observations', value=json_df)
    print(sys.getsizeof(json_df), "bytes of data is pushed to xcom")

def extract_location(**context):
    """
    - pull the raw data to extract the site location coordinates
    - aggregate the data and create dataframe
    - convert data frame into dictionary and push to the next task through xcoms
    """
    ti = context['ti']
    data = ti.xcom_pull(key='wow_data')
    print(sys.getsizeof(data), "bytes of data is pulled from xcom")
    lon = []
    lat = []
    siteId = []
    for item in data["features"]:
        lon.append(item["geometry"]["coordinates"][0]) # extract longitude
        lat.append(item["geometry"]["coordinates"][1]) # extract latitude
        siteId.append(item["properties"])
    # normalize dictionary to flat table
    df1 = pd.json_normalize(siteId)
    df2 = pd.DataFrame({"longitude": lon, "latitude": lat})
    # join two dataframes
    df = pd.DataFrame(df1['siteId']).join(df2)
    json_df = df.to_json(orient="records")
    # push to xcom
    ti.xcom_push(key='extract_location', value=json_df)
    

def transform_location(**context):
    """
    - Reserve geocode coordinates into physical address
    - create dataframe and rename table columns 
    - push table to the loading stage
    """
    ti = context['ti']
    str_df = ti.xcom_pull(key='extract_location')
    dict_df = json.loads(str_df)
    df = pd.DataFrame(dict_df)
    # join the latitude and longitude columns of the dataframe
    lat_long = [', '.join(str(x) for x in y) for y in map(
        tuple, df[['latitude', 'longitude']].values)]
    xy = []
    results = []
    for i in lat_long:
        x = float(i.split(",")[0])  # latitute
        y = float(i.split(",")[1])  # longitude
        xy.append((x,y)) # create and append tuple

    # reverse geocode the longitude and latitude to phsyical address (country name, city name, etc)
    results = [rg.search(i) for i in xy]
    #normalize the result of the reverse geocoding
    df2 = pd.json_normalize(results)
    # rename table columns
    df2 = df2.rename(columns={'lat': 'latitude', 'lon': 'longitude', 'name': 'city',
                            'admin1': 'region', 'admin2': 'local_region', 'cc': 'country_code'})
    # join 2 dataframes horizontally
    df = pd.DataFrame(df['siteid']).join(df2)
    # push to xcom
    json_df = df.to_json(orient="records")
    ti.xcom_push(key='transform_location', value=json_df)

def extract_date(**context):
    """
    - extract the datetime field from the data
    - normalize to pandas dataframe
    - push to xcom for processing
    """
    ti = context['ti']
    # pull data
    data = ti.xcom_pull(key='wow_data')
    report_datetime = []
    # iterate through the list of dictionaries
    for item in data["features"]:
        report_datetime.append(item["properties"])
    # flatten the list of dictionaries
    df = pd.json_normalize(report_datetime)
    cols = ["reportId", "reportEndDateTime"]
    # drop unwanted columns
    for col in df.columns:
        if col not in cols:
            df = df.drop(columns=[col])
    # convert dataframe to dictionary
    json_df = df.to_json(orient="records")
    ti.xcom_push(key='extract_date', value=json_df)

def transform_datetime(**context):
    """
    - convert datetime into reusable format
    - process year, month, day and time from the datetime
    - push to xcom for loading
    """
    ti = context['ti']
    str_df = ti.xcom_pull(key='extract_date')
    dict_df = json.loads(str_df)
    df = pd.DataFrame(dict_df)
    df['reportEndDateTime'] = df['reportEndDateTime'].apply(convert_datetime)
    df['date'] = df['reportEndDateTime'].dt.strftime('%Y-%m-%d')
    df['year'] = df['reportEndDateTime'].dt.year #extract year
    df['month'] = df['reportEndDateTime'].dt.month #extract month
    df['day'] = df['reportEndDateTime'].dt.day
    df['time'] = df['reportEndDateTime'].dt.time
    df = df.drop(columns=['reportEndDateTime'])
    json_df = df.to_json(orient="records")
    ti.xcom_push(key='transform_date', value=json_df)

def load_dataframe_to_redshift(data_frame, table_name):
    """
    - takes dataframe and convert it to flat file (csv)
    - use boto3 library to connection to AWS
    - upload the csv file to dedicated S3 bucket. S3 serves as the data lake
    - copy the uploaded data into Redshift using the COPY command
    """
    # establish connection to redshift
    pr.connect_to_redshift(dbname=Variable.get('DBNAME'),
                            host=Variable.get('AWS_REDSHIFT_HOST'),
                            port=Variable.get('REDSHIFT_PORT'),
                            user=Variable.get('REDSHIFT_USER'),
                            password=Variable.get("AWS_REDSHIFT_PWD"))
    # establish connection to S3
    pr.connect_to_s3(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        bucket='wow-data-msc-project',
        subdirectory=table_name)
    # copy data from S3 to RedShift
    # if append is False new table will be created if it exists or not. 
    # Meaning it will drop existing tabel to create new one.
    # And if it's true data inflow into the data warehouse will be on incremental basis 
    pr.pandas_to_redshift(data_frame, table_name, column_data_types=None, index=False, save_local=False,
                            delimiter=',', quotechar='"', dateformat='auto', timeformat='auto', region='us-east-1', append=True)
    pr.close_up_shop()

def load_observations(**context):
    ti = context['ti']
    # pull processed data
    str_df = ti.xcom_pull(key='transform_observations')
    dic_df = json.loads(str_df)
    df = pd.DataFrame(dic_df)
    # load dataframe to redshift
    load_dataframe_to_redshift(data_frame=df, table_name='observations')
    print(df.shape[0], "rows has been added to the wow_database.public.observations")


def load_location(**context):
    ti = context['ti']
    str_df = ti.xcom_pull(key='extract_location')
    dic_df = json.loads(str_df)
    df = pd.DataFrame(dic_df)
    load_dataframe_to_redshift(data_frame=df, table_name='location')
    print(df.shape[0], "rows has been added to the wow_database.public.location")

def load_date(**context):
    ti = context['ti']
    str_df = ti.xcom_pull(key='transform_date')
    dic_df = json.loads(str_df)
    df = pd.DataFrame(dic_df)
    load_dataframe_to_redshift(data_frame=df, table_name='datetime')
    print(df.shape[0], "rows has been added to the wow_database.public.datetime")

@provide_session
def cleanup_xcom(session=None, **context):
    """
    This function clean all the data generated as a result of data sharing with xcom.
    If not deleted it could cause airflow to crash or slow down.
    Since the data are extracted and stored into the data warehouse. The data in the 
    xcoms are not longer required.
    """
    dag = context["dag"]
    dag_id = dag._dag_id 
    #query and delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

with DAG(
    dag_id="WOW_data_ETL_and_data_warehouse",
    description='The data pipeline will first fetch wow data, extract features and values; transform the values; create pandas dataframe and finally load the dataframes to S3 and copy to RedShift.',
    start_date=datetime(2022, 7, 28),
    schedule_interval= "0 */1 * * *", #cron expression set to hourly
    catchup=False,
    tags=["Real-time ETL"]
) as dag:

    ingest_data = PythonOperator(
        task_id="fetch_wow_data", python_callable=get_data,)
    #extraction
    with TaskGroup("extract", tooltip="extract_data") as extract:
        extract_observation_table = PythonOperator(
            task_id="extract_observations", 
            python_callable=extract_observations,
            provide_context=True,)
        extract_location_table = PythonOperator(
            task_id="extract_location", 
            python_callable=extract_location,
            provide_context=True,)
        extract_date_table = PythonOperator(
            task_id="extract_datetime",
            python_callable=extract_date,
            provide_context=True,)
        
        extract_observation_table
        extract_location_table
        extract_date_table
    #transformation
    with TaskGroup("transform", tooltip="transform_data") as transform:
        transform_observation_table = PythonOperator(
            task_id = "transform_observations",
            python_callable=transform_observations,
            provide_context = True,)
        transform_location_table = PythonOperator(
            task_id="transform_location",
            python_callable=transform_location,
            provide_context=True,)
        transform_datetime_table = PythonOperator(
            task_id="transform_datetime",
            python_callable=transform_datetime,
            provide_context=True,)

        transform_observation_table
        transform_location_table
        transform_datetime_table
    #loading
    with TaskGroup("load", tooltip="load data to S3 and copy to RedShift") as load:
        load_observation_table = PythonOperator(
            task_id="load_observations", 
            python_callable=load_observations,
            provide_context=True,)
        load_location_table = PythonOperator(
            task_id="load_location", 
            python_callable=load_location,
            provide_context=True,)
        load_date_table = PythonOperator(
            task_id="load_datetime",
            python_callable=load_date,
            provide_context=True,)

        load_observation_table
        load_location_table
        load_date_table
    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable=cleanup_xcom,
        provide_context=True,)

    ingest_data >> extract >> transform >> load >> clean_xcom

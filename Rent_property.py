from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import json
import requests
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

#I assign a new variable that will show me the date for every new extraction. It will assign a new file name for each new extraction.
#which will consist of date and time
now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

#Define the S3 bucket where already transformed data will be loaded and landed.
s3_bucket = 'ready-togo-csv-bucket'

#Creating first function that will fetch data from the Rapid API
def extract_property_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['data_string']
    #return headers
    response = requests.get(url,headers=headers,params=querystring)
    response_data = response.json()

    #specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    #Write the JSON response to a file
    with open(output_file_path,'w') as output_file:
        json.dump(response_data,output_file,indent=4) #indent for pretty formatting
    output_list = [output_file_path,file_str]
    return output_list


#Assign necessary arguments for the Dag 
default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,8,1),
    'email':['myemail@domain.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(seconds=15)
}

#Load JSON config file (that key you will get on the RapidApi website). Just fetch them.
#Create a new JSON file and put inside those keys (you will have two of them).
with open('/home/ubuntu/airflow/config_api.json','r') as config_file:
    api_host_key=json.load(config_file)


#Creating Dag
with DAG('price_analytics_dag',#Name of the dag
        default_args=default_args,#Arguments that were assigned previously          
        schedule_interval="@daily",#That means each day at 00.00, data will fetch from the API.
        catchup=False) as dag:#Preventing for backfilling
        #For example, if you're loading data from some source that is only updated hourly into your database, 
        # backfilling, which occurs in rapid succession, would just be importing the same data again and again.

        #First task: Scrapping data from Rapid Api website
        extract_stock_price_data = PythonOperator(
            task_id='tsk_extract_property_data',
            python_callable=extract_property_data,
            op_kwargs={'url': "https://realty-mole-property-api.p.rapidapi.com/rentalListings",'querystring': {"city":"New York","state":"NY","limit":"500"},
                        'headers':api_host_key,'data_string':dt_now_string}# This information you will have from Rapid APi website 
        )
        #Loading extracted data
        load_to_s3 = BashOperator(
            task_id = 'tsk_load_to_s3',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_property_data")[0]}} s3://rent-property-user/'
        )
        #Let's create another task that show (monitor) if we will have any ready-to-go CSV files in our bucket "ready-to-go-csv-bucket" in AWS (buckets).
        is_file_in_s3_bucket = S3KeySensor(
            task_id = 'tsk_is_file_in_s3_bucket',
            bucket_key = '{{ti.xcom_pull("tsk_extract_property_data")[1]}}',
            bucket_name = s3_bucket,
            aws_conn_id = 'aws_s3_conn',#This will connect our airflow to AWS
            wildcard_match = False, #Set this to True if u want to use wildcards in the prefix
            timeout = 60,
            poke_interval = 5 #The interval between S3 checks in sec
        )
        #Next step is to upload data to Amazon RedShift data werehouse
        upload_data_to_redshift = S3ToRedshiftOperator(
            task_id = 'tsk_transfer_s3_to_redshift',
            aws_conn_id = 'aws_s3_conn',
            redshift_conn_id = 'conn_id_redshift',
            s3_bucket=s3_bucket,
            s3_key='{{ti.xcom_pull("tsk_extract_property_data")[1]}}',
            schema = "PUBLIC",
            table = 'propertydata',#Name of the table in RedShift Query Editor
            copy_options = ["csv IGNOREHEADER 1"]#To load a CSV into Redshift with a header
        )


        extract_stock_price_data >> load_to_s3 >> is_file_in_s3_bucket >> upload_data_to_redshift
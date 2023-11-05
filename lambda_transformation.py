import json
import boto3
#If u will have problem such as "lambda_function" cannot import "pandas, etc" module
#So u must add a Layers with package that u want to use in ur Lambda function
import pandas as pd 

s3_client= boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'ready-togo-csv-bucket'
    target_file_name = object_key[:-5]#Will return a file with name consist last 5 letters from original json file storage in our buckets
    #so i will be like. 'response_data_30102023194057.json' -> '7.json' 
    
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    data = response['Body']
    data = response['Body'].read().decode('utf-8')
    data = json.loads(data)
    f = []
    #Featching all dictionaries from a json file and append it to a new one
    #Converting to pandas DataFrame
    for i in data:
        f.append(i)
    df = pd.DataFrame(f)
    #Select necessary  columns
    selected_columns = ['bedrooms','bathrooms','squareFootage','propertyType',
                        'yearBuilt','price','daysOnMarket','zipCode']
    
    df = df[selected_columns]
    #Convert dataframe to CSV file
    csv_data = df.to_csv(index=False)
    #Upload CSV to s3
    bucket_name=target_bucket
    object_key = f"{target_file_name}.csv"
    s3_client.put_object(Bucket= bucket_name,Key=object_key,Body=csv_data)

    return{
        "statusCode":200,
        "body": json.dumps('CSV conversion and S3 upload completed!')
    }
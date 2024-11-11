import json
import boto3
import pandas as pd
from dynamodb_utils import get_dynamodb_table, insert_item

# Load configurations
with open('../config/s3_config.json') as f:
    s3_config = json.load(f)

with open('../config/dynamodb_config.json') as f:
    dynamodb_config = json.load(f)

s3 = boto3.client('s3')

def extract_data():
    obj = s3.get_object(Bucket=s3_config['bucket_name'], Key=s3_config['key'])
    return pd.read_csv(obj['Body'])

def transform_data(df):
    df['TotalPrice'] = df['Quantity'] * df['Price']
    return df[['TransactionID', 'UserID', 'ProductID', 'TotalPrice', 'Date']]

def load_data(df):
    table = get_dynamodb_table(dynamodb_config['table_name'])
    for _, row in df.iterrows():
        item = {
            'TransactionID': str(row['TransactionID']),
            'UserID': str(row['UserID']),
            'ProductID': str(row['ProductID']),
            'TotalPrice': str(row['TotalPrice']),
            'Date': row['Date']
        }
        insert_item(table, item)

def run_etl():
    df = extract_data()
    transformed_df = transform_data(df)
    load_data(transformed_df)

if __name__ == "__main__":
    run_etl()

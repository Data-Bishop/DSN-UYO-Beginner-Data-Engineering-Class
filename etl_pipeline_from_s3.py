import pandas
import sqlalchemy
import boto3
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_csv(bucket_name, prefix=''):
    s3_client = boto3.client('s3')
    dataframes = []
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        for object in response.get('Contents', []):
            if object['Key'].endswith('.csv'):
                try:
                    csv_object = s3_client.get_object(Bucket=bucket_name, Key = object['Key'])
                    body = csv_object['Body'].read().decode('utf-8')
                except Exception as error:
                    logger.error(f"Failure to get and read object body. Error message: {error}")
                    
                body_buffer = StringIO(body)

                dataframe = pandas.read_csv(body_buffer)
                dataframes.append(dataframe)
                logger.info("Data extracted successfully")
        return dataframes
    except Exception as error:
        logger.error(f"Failure to Extract Data. Error message: {error}")

def transform_data(dataframes):
    new_dataframe = pandas.concat(dataframes, ignore_index=True)
    
    # Filling Missing Values
    new_dataframe['Product'] = new_dataframe['Product'].fillna('Unknown')
    new_dataframe['Category'] = new_dataframe['Category'].fillna('Unknown')
    new_dataframe['Quantity'] = new_dataframe['Quantity'].fillna(0)
    new_dataframe['Price'] = new_dataframe['Price'].fillna(0)
    new_dataframe['Sale_Date'] = new_dataframe['Sale_Date'].fillna(new_dataframe['Sale_Date'].mode())
    new_dataframe['Customer_Region'] = new_dataframe['Customer_Region'].fillna('Unknown')
    
    # Handling Duplicates
    new_dataframe = new_dataframe.drop_duplicates()
    
    # Formatting text columns
    new_dataframe['Product'] = new_dataframe['Product'].str.strip()
    new_dataframe['Category'] = new_dataframe['Category'].str.strip()
    new_dataframe['Customer_Region'] = new_dataframe['Customer_Region'].str.strip()
    
    new_dataframe['sales_amount'] = new_dataframe['Quantity'] * new_dataframe['Price']
    new_dataframe['Sale_Date'] = pandas.to_datetime(new_dataframe['Sale_Date'])
    
    return new_dataframe

def load_to_postgres(dataframe, connection_string):
    try:
        engine = sqlalchemy.create_engine(connection_string)
        
        dataframe.to_sql('sales_transactions', engine, if_exists='append')
        logger.info("Data Loaded to Postgresql successfully")
    except Exception as error:
        logger.error(f"Failed to load to postgresql. Error message: {error}")
    
def main():
    CONNECTION_STRING = 'postgresql://databishop:databishop@localhost:5432/sales_database'
    BUCKET_NAME = "databishop-messy-data"
    PREFIX = ""
    
    
    extracted_data = extract_csv(BUCKET_NAME, PREFIX)
    transformed_data = transform_data(extracted_data)
    load_to_postgres(transformed_data, CONNECTION_STRING)
    
    print("ETL Pipeline is successful")
    print(transformed_data)
    
if __name__ == '__main__':
    main()
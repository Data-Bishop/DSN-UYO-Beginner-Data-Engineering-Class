import pandas
import sqlalchemy

def extract_csv():
    dataframe = pandas.read_csv("./Messy/sales-jan.csv")
    return dataframe

def transform_data(dataframe):
    
    new_dataframe = dataframe.copy()
    
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
    engine = sqlalchemy.create_engine(connection_string)
    
    dataframe.to_sql('sales_transactions', engine, if_exists='append')
    
def main():
    CONNECTION_STRING = 'postgresql://databishop:databishop@localhost:5432/sales_database'
    
    extracted_data = extract_csv()
    transformed_data = transform_data(extracted_data)
    load_to_postgres(transformed_data, CONNECTION_STRING)
    
    print("ETL Pipeline is successful")
    print(transformed_data)
    
if __name__ == '__main__':
    main()
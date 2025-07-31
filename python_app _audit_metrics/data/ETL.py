

import pandas as pd
#import numpy as np
import pyodbc
from datetime import datetime

def StartTime():
    start_time = datetime.now()
    return start_time


def LoadCSVs():
    #Define the file paths
    csv_file_customers = r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library SystemCustomers.csv'
    csv_file_books = r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library Systembook.csv'

    #Load the file path into a data frame
    df_books = pd.read_csv(csv_file_books)
    df_customers = pd.read_csv(csv_file_customers)

    #Return files
    return df_books, df_customers

def CleanCustomerFile(df_customers):

    #Count rows:
    Customer_rows = len(df_customers)

    #Remove duplicates:
    Customer_duplicates = df_customers.duplicated().sum()  #Stores number of duplicates in a variable before deleting them.
    df_customers_cleaned = df_customers.drop_duplicates().reset_index(drop =  True)
    

    #Drop NA rows
    Customer_na_rows = df_customers.isna().any(axis=1).sum()  #count the number of rows with nas in a variable before deleting them.
    df_customers_cleaned = df_customers_cleaned.dropna().reset_index(drop =  True)

    #Ensure consistency of data types
    df_customers_cleaned['Customer ID'] = df_customers_cleaned['Customer ID'].astype('int64')
    df_customers_cleaned['Customer Name'] = df_customers_cleaned['Customer Name'].astype('string')

    return df_customers_cleaned, Customer_rows, Customer_duplicates, Customer_na_rows

def cleanBooksFile(df_books):

    #Count rows:
    Books_rows = len(df_books)

    # Remove duplicates:
    Books_duplicates = df_books.duplicated().sum()  #Stores number of duplicates in a variable before deleting them.
    df_books_cleaned = df_books.drop_duplicates()

    # Save nan values in a new df before deleting them:
    df_books_nan = df_books[df_books.isna().any(axis=1)]

    #Drop NA rows:
    Books_na_rows = df_books.isna().any(axis=1).sum() 
    df_books_cleaned = df_books_cleaned.dropna()

    #Ensure consistency of data types:
    df_books_cleaned['Id'] = df_books_cleaned['Id'].astype('int64')
    df_books_cleaned['Books'] = df_books_cleaned['Books'].astype('string')

    df_books_cleaned['Book checkout'] = df_books_cleaned['Book checkout'].str.strip()
    df_books_cleaned['Book checkout'] = df_books_cleaned['Book checkout'].str.replace('"','', regex = False)
    df_books_cleaned['Book checkout'] = pd.to_datetime(df_books_cleaned['Book checkout'],errors='coerce')
    df_books_cleaned['Book checkout'] = df_books_cleaned['Book checkout'].fillna(pd.Timestamp('1900-01-01'))

    df_books_cleaned['Book Returned'] = df_books_cleaned['Book Returned'].str.strip()
    df_books_cleaned['Book Returned'] = pd.to_datetime(df_books_cleaned['Book Returned'],errors='coerce')

    df_books_cleaned['Days allowed to borrow'] = df_books_cleaned['Days allowed to borrow'].astype('string')
    df_books_cleaned['Customer ID'] = df_books_cleaned['Customer ID'].astype('int64')

    return df_books_cleaned, Books_rows, Books_duplicates, Books_na_rows

def AddCalculatedColumns(df_books_cleaned):
    #Add calculated columns:

    df_books_cleaned['days between checkout and return'] = (df_books_cleaned['Book Returned'] - df_books_cleaned['Book checkout']).dt.days
    df_books_cleaned['days between checkout and return'] = df_books_cleaned['days between checkout and return'].astype('Int64')

    return df_books_cleaned



def AuditData(start_time, rows1, rows2, nas1, nas2, duplicates1, duplicates2):
    #Create and audit data frame
    # Define the column names
    columns = [
        "data pipeline",
        "execution start time",
        "execution end time",
        "source name",
        "source type",
        "number of rows",
        "NA rows",
        "Duplicate rows",
        "destination table",
        "rows inserted in destination table"
    ]

    # Create an empty DataFrame with those columns
    df_audit = pd.DataFrame(columns=columns)

    end_time = datetime.now()
    
    # Add a row of values
    df_audit.loc[0] = [
        "Library ETL Pipeline",               # data pipeline
        start_time,                 # execution start time
        end_time,                 # execution end time
        "03_Library SystemCustomers",                  # source name
        "CSV",                                 # source type
        rows1,                                  # number of rows
        nas1,                                   # NA rows
        duplicates1,                                    # Duplicate rows
        "Customers_extract",                       # destination table
        0                                   # rows inserted in destination table
    ]

    # Add a row of values
    df_audit.loc[1] = [
        "Library ETL Pipeline",               # data pipeline
        start_time,                 # execution start time
        end_time,                 # execution end time
        "03_Library Systembooks",                  # source name
        "CSV",                                 # source type
        rows2,                                  # number of rows
        nas2,                                   # NA rows
        duplicates2,                                    # Duplicate rows
        "Books_extract",                       # destination table
        0                                   # rows inserted in destination table
    ]

    return df_audit



def IngestIntoDB(df_customers_cleaned,df_books_cleaned,audit_df):

    #Connect to SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=STUDENT06;'
        'DATABASE=LibraryBronceDB;'
        'Trusted_Connection=yes;'
    )

    cursor = conn.cursor()

    # Insert Dataframe Customers into SQL Server:
    cursor.execute("TRUNCATE TABLE Customers_extract")
    for index, row in df_customers_cleaned.iterrows():
        cursor.execute("INSERT INTO Customers_extract ([Customer ID],[Customer Name]) values(?,?)", row['Customer ID'], row['Customer Name'])

    # Insert Dataframe Customers into SQL Server:
    cursor.execute("TRUNCATE TABLE Books_extract")
    for index, row in df_books_cleaned.iterrows():
         cursor.execute("INSERT INTO Books_extract ([Id],[Books],[Book checkout], [Book Returned], [Days allowed to borrow], [Customer ID], [days between checkout and return]) values(?,?,?,?,?,?,?)", row['Id'], row['Books'],row['Book checkout'], row['Book Returned'], row['Days allowed to borrow'], row['Customer ID'], row['days between checkout and return'])


    # Insert audit data into SQL Server:
    for index, row in audit_df.iterrows():
         cursor.execute(
        "INSERT INTO DataPipelineLog ([data pipeline], [execution start time], [execution end time], [source name], [source type], [number of rows], [NA rows], [Duplicate rows], [destination table], [rows inserted in destination table]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        row["data pipeline"],
        row["execution start time"],
        row["execution end time"],
        row["source name"],
        row["source type"],
        row["number of rows"],
        row["NA rows"],
        row["Duplicate rows"],
        row["destination table"],
        row["rows inserted in destination table"]
        )

    conn.commit()
    cursor.close()




if __name__ == "__main__":

    start_time = StartTime()
    df_books, df_customers = LoadCSVs()  
    df_customers_cleaned, Customer_rows, Customer_duplicates, Customer_na_rows = CleanCustomerFile(df_customers)
    df_books_cleaned, Books_rows, Books_duplicates, Books_na_rows = cleanBooksFile(df_books)
    df_books_cleaned = AddCalculatedColumns(df_books_cleaned)
    df_audit = AuditData(start_time,Customer_rows,Books_rows,Customer_na_rows,Books_na_rows,Customer_duplicates,Books_duplicates)
    IngestIntoDB(df_customers_cleaned,df_books_cleaned, df_audit)





import pandas as pd
#import numpy as np
import pyodbc

def LoadCSVs():
    #Define the file paths
    csv_file_customers = r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library SystemCustomers.csv'
    csv_file_books = r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library Systembook.csv'

    #Load the file path into a data frame
    df_books = pd.read_csv(csv_file_books)
    df_customers = pd.read_csv(csv_file_customers)

    return df_books, df_customers

def CleanCustomerFile(df_customers):

    #Remove duplicates:
    df_customers_cleaned = df_customers.drop_duplicates().reset_index(drop =  True)

    #Drop NA rows
    df_customers_cleaned = df_customers_cleaned.dropna().reset_index(drop =  True)

    #Ensure consistency of data types
    df_customers_cleaned['Customer ID'] = df_customers_cleaned['Customer ID'].astype('int64')
    df_customers_cleaned['Customer Name'] = df_customers_cleaned['Customer Name'].astype('string')

    return df_customers_cleaned

def cleanBooksFile(df_books):
    # Remove duplicates:
    df_books_cleaned = df_books.drop_duplicates()

    # Save nan values in a new df before deleting them:
    df_books_nan = df_books[df_books.isna().any(axis=1)]

    #Drop NA rows:
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

    return df_books_cleaned

def AddCalculatedColumns(df_books_cleaned):
    #Add calculated columns:

    df_books_cleaned['days between checkout and return'] = (df_books_cleaned['Book Returned'] - df_books_cleaned['Book checkout']).dt.days
    df_books_cleaned['days between checkout and return'] = df_books_cleaned['days between checkout and return'].astype('Int64')

    return df_books_cleaned

def IngestIntoDB(df_customers_cleaned,df_books_cleaned):

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
    conn.commit()
    cursor.close()


if __name__ == "__main__":
    df_books, df_customers = LoadCSVs()  
    df_customers_cleaned = CleanCustomerFile(df_customers)
    df_books_cleaned = cleanBooksFile(df_books)
    df_books_cleaned = AddCalculatedColumns(df_books_cleaned)
    IngestIntoDB(df_customers_cleaned,df_books_cleaned)



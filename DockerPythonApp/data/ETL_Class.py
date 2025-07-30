########### CLASSES TEMPLATE

import pandas as pd
import pyodbc

class myFirstFClass():
    def __init__(self, CustomersFilePath, BooksFilePath):
        self.CustomersFile = CustomersFilePath
        self.BooksFile = BooksFilePath

    def LoadCSVs(self):
        #Load the file path into a data frame
        df_books = pd.read_csv(self.BooksFile)
        df_customers = pd.read_csv(self.CustomersFile)
        return df_books, df_customers
    
    def CleanCustomerFile(df_customers):
        #Remove duplicates:
        df_customers.drop_duplicates()
        #Drop NA rows
        df_customers_cleaned = df_customers.dropna()
        #Ensure consistency of data types
        df_customers_cleaned['Customer ID'] = df_customers_cleaned['Customer ID'].astype('int64')
        df_customers_cleaned['Customer Name'] = df_customers_cleaned['Customer Name'].astype('string')
        return df_customers_cleaned


    def main(self):
         df_books, df_customers = self.LoadCSVs()  
         df_customers_cleaned = self.CleanCustomerFile(df_customers)

if __name__ == '__main__':
    x = myFirstFClass(r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library SystemCustomers.csv',r'C:\Users\Admin\Desktop\Module5\python_app\data\03_Library Systembook.csv')
    x.main() 
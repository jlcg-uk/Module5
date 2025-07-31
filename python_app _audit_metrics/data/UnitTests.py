import pandas as pd
import unittest
from pandas.testing import assert_frame_equal
from ETL import CleanCustomerFile, AddCalculatedColumns


class TestETL(unittest.TestCase):
    def setUp(self):
        data_source = {
            'Customer ID': [1, 2, 2, 3, 3, 4],
            'Customer Name': ['Jose', 'Luis', 'Luis', 'Daniel', 'Daniel', 'Oscar']
        }
        data_output = {
            'Customer ID': [1, 2, 3, 4],
            'Customer Name': ['Jose', 'Luis', 'Daniel', 'Oscar']
        }

        books_date_source = {
            'Book checkout': ['20/03/2023', '12/04/2023', '10/05/2023'],
            'Book Returned': ['01/04/2023', '20/05/2023', '20/05/2023']
        }

        books_dates_output = {
            'Book checkout': ['20/03/2023', '12/04/2023', '10/05/2023'],
            'Book Returned': ['01/04/2023', '20/05/2023', '20/05/2023'],
            'days between checkout and return': ['12', '38', '10'],    
        }

        self.df_Customers = pd.DataFrame(data_source)
        self.df_CustomersCleaned = pd.DataFrame(data_output)
        self.df_CustomersCleaned['Customer Name'] = self.df_CustomersCleaned['Customer Name'].astype('string')  #Make sure that the Customer name column is of the same type as in the ETL.py function.

        self.df_DatesSource = pd.DataFrame(books_date_source)
        self.df_DatesSource['Book checkout'] = pd.to_datetime(self.df_DatesSource['Book checkout'], format='%d/%m/%Y')
        self.df_DatesSource['Book Returned'] = pd.to_datetime(self.df_DatesSource['Book Returned'], format='%d/%m/%Y')
        self.df_DatesOutput = pd.DataFrame(books_dates_output)
        self.df_DatesOutput['Book checkout'] = pd.to_datetime(self.df_DatesOutput['Book checkout'], format='%d/%m/%Y')
        self.df_DatesOutput['Book Returned'] = pd.to_datetime(self.df_DatesOutput['Book Returned'], format='%d/%m/%Y')
        self.df_DatesOutput['days between checkout and return'] = self.df_DatesOutput['days between checkout and return'].astype('Int64')

    def test_RemoveDuplicates(self):
        assert_frame_equal(CleanCustomerFile(self.df_Customers),self.df_CustomersCleaned)

    def test_AddCalculation(self):
        assert_frame_equal(AddCalculatedColumns(self.df_DatesSource),self.df_DatesOutput)

    
    def tearDown(self):
        print('All tests ran successfully.')


if __name__ == '__main__':
    unittest.main()
        




import pandas as pd
import unittest
from ETL import CleanCustomerFile

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

        self.df_Customers = pd.DataFrame(data_source).reset_index()
        self.df_CustomersCleaned = pd.DataFrame(data_output)
#        self.RemoveDuplicates = CleanCustomerFile()

    def test_RemoveDuplicates(self):
        self.assertEqual(CleanCustomerFile(self.df_Customers),self.df_CustomersCleaned,'Duplicates were not removed')

    
    def tearDown(self):
        print('All tests ran successfully.')


if __name__ == '__main__':
    unittest.main()
        




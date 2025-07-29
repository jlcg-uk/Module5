import pandas as pd
import unittest
from pandas.testing import assert_frame_equal
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

        self.df_Customers = pd.DataFrame(data_source)
        self.df_CustomersCleaned = pd.DataFrame(data_output)
        self.df_CustomersCleaned['Customer Name'] = self.df_CustomersCleaned['Customer Name'].astype('string')  #Make sure that the Customer name column is of the same type as in the ETL.py function.

    def test_RemoveDuplicates(self):
        assert_frame_equal(CleanCustomerFile(self.df_Customers),self.df_CustomersCleaned)

    
    def tearDown(self):
        print('All tests ran successfully.')


if __name__ == '__main__':
    unittest.main()
        




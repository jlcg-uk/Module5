

import pandas as pd
from pandas.testing import assert_frame_equal
from ETL import CleanCustomerFile

data_source = {
            'Customer ID': [1, 2, 2, 3, 3, 4],
            'Customer Name': ['Jose', 'Luis', 'Luis', 'Daniel', 'Daniel', 'Oscar']
        }

data_output = {
            'Customer ID': [1, 2, 3, 4],
            'Customer Name': ['Jose', 'Luis', 'Daniel', 'Oscar']
        }

df_Customers = pd.DataFrame(data_source).drop_duplicates().reset_index(drop =  True)
df_CustomersCleaned = pd.DataFrame(data_output)

print(df_Customers)
print(CleanCustomerFile(df_Customers))
print(df_CustomersCleaned)

assert_frame_equal(df_Customers,CleanCustomerFile(df_Customers))

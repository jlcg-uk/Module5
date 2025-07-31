import pandas as pd

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

# Create an empty DataFrame
df = pd.DataFrame(columns=columns)

# Add a row of values
df.loc[0] = [
    "Customer ETL Pipeline",               # data pipeline
    "2025-07-31 08:00:00",                 # execution start time
    "2025-07-31 08:10:00",                 # execution end time
    "customers_2025.csv",                  # source name
    "CSV",                                 # source type
    5000,                                  # number of rows
    120,                                   # NA rows
    35,                                    # Duplicate rows
    "dim_customers",                       # destination table
    4845                                   # rows inserted in destination table
]

# Display the DataFrame
print(df)



from datetime import datetime

current_time = datetime.now()
print(current_time)
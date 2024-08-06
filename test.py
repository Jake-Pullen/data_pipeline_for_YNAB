import polars as pl
entities = ['accounts', 'categories', 'months', 'payees', 'transactions', 'scheduled_transactions']
# Define the path to the transactions parquet file


#file_path = 'data/base/categories.parquet'
file_path = 'data/base/accounts.parquet'

# Read the parquet file into a polars DataFrame
transactions_df = pl.read_parquet(file_path)

# Display the DataFrame
print(transactions_df)
import polars as pl

entities = ['accounts', 'categories', 'months', 'payees', 'transactions', 'scheduled_transactions']

for entity in entities:
    print(f"Processing entity: {entity}")
    file_path = f'data/base/{entity}.parquet'
    # Read the parquet file into a polars DataFrame
    entity_df = pl.read_parquet(file_path)
    # Print the schema of the DataFrame
    print(f"Schema of {entity} DataFrame:")
    print(entity_df.schema)
    # Display the first few rows of the DataFrame
    print(f"First few rows of {entity} DataFrame:")
    print(entity_df.head())

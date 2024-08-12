import polars as pl

df = pl.read_parquet('data/warehouse/transactions.parquet')
print("Data loaded from Parquet file:")
print(df)

relevant_data = df.sql('''
    SELECT 
        date,
        sum(transaction_amount) as total
    FROM self
    GROUP BY date
    ORDER BY date DESC
    '''
)
print("Data after SQL query:")
print(relevant_data)
import polars as pl
class DimAccounts:
    def __init__(self, config):
        self.config = config
        self.transform()

    def transform(self):
        file_path = self.config['base_data_path'] + '/accounts.parquet'
        # Read the parquet file into a polars DataFrame
        accounts_df = pl.read_parquet(file_path)

        # Transform the DataFrame
        accounts_df = (
            accounts_df
            .with_columns([
                pl.col("id").alias("account_id"),
                pl.col("name").alias("account_name"),
                pl.col("type").alias("account_type"),
                pl.col("on_budget").alias("on_budget"),
                pl.col("closed").alias("closed"),
                pl.col("note").alias("note"),
                pl.col("balance").alias("balance"),
                pl.col("cleared_balance").alias("cleared_balance"),
                pl.col("uncleared_balance").alias("uncleared_balance"),
                pl.col("deleted").alias("deleted"),
            ])
            .with_columns([
                pl.col("note").fill_null("unknown"),
                (pl.col("balance") / 100).alias("balance"),
                (pl.col("cleared_balance") / 100).alias("cleared_balance"),
                (pl.col("uncleared_balance") / 100).alias("uncleared_balance"),
            ])
            .drop([
                "transfer_payee_id", "direct_import_linked", "direct_import_in_error",
                "last_reconciled_at", "debt_original_balance", "debt_interest_rates",
                "debt_minimum_payments", "debt_escrow_amounts", "ingestion_date"
            ])
        )
        # Write the DataFrame to a new parquet file
        accounts_df.write_parquet(self.config['warehouse_data_path'] + '/accounts.parquet')


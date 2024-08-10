import polars as pl
import logging
import os

class Facts:
    def __init__(self, config):
        self.config = config
        self.base_file_path = self.config['base_data_path']
        os.makedirs(self.config['warehouse_data_path'], exist_ok=True)
        
    def get_full_file_path(self, file_name):
        return f"{self.base_file_path}/{file_name}"
    
class FactTransactions(Facts):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('transactions.parquet')
        self.transform()

    def transform(self):
        # Read the parquet file into a polars DataFrame
        try:
            transactions_df = pl.read_parquet(self.file_path)
        except FileNotFoundError:
            logging.error("The transactions DataFrame does not exist")
            return
        
        # Transform the DataFrame
        logging.info("Transforming the transactions DataFrame")
        try:
            transactions_df = (
                transactions_df
                .with_columns([
                    pl.col("id").alias("transaction_id"),
                    pl.col("date").alias("transaction_date"),
                    pl.col("amount").alias("transaction_amount"),
                    pl.col("memo").alias("transaction_memo"),
                    pl.col("cleared").alias("transaction_cleared"),
                    pl.col("approved").alias("transaction_approved"),
                    pl.col("flag_color").alias("transaction_flag_color"),
                    pl.col("account_id").alias("account_id"),
                    pl.col("payee_id").alias("payee_id"),
                    pl.col("category_id").alias("category_id"),
                    pl.col("transfer_account_id").alias("transfer_account_id"),
                ])
                .with_columns([
                    pl.col("memo").fill_null("unknown"),
                    (pl.col("amount") / 100).alias("transaction_amount"),
                ])
                .drop([
                    "transfer_transaction_id", "matched_transaction_id", "import_id",
                    "subtransactions", "deleted","flag_name","account_name",
                    "payee_name","category_name","import_payee_name","import_payee_name_original",
                    "debt_transaction_type","ingestion_date"
                ])
            )
        except Exception as e:
            logging.error(f"Failed to transform the transactions DataFrame: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed transactions DataFrame to parquet file")
        try:
            transactions_df.write_parquet(self.config['warehouse_data_path'] + '/transactions.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed transactions DataFrame: {e}")

class FactScheduledTransactions(Facts):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('scheduled_transactions.parquet')
        self.transform()

    def transform(self):
        # Read the parquet file into a polars DataFrame
        try:
            scheduled_transactions_df = pl.read_parquet(self.file_path)
        except FileNotFoundError:
            logging.error("The scheduled transactions DataFrame does not exist")
            return

        # Transform the DataFrame
        logging.info("Transforming the scheduled transactions DataFrame")
        try:
            scheduled_transactions_df = (
                scheduled_transactions_df
                .with_columns([
                    pl.col("id").alias("scheduled_transaction_id"),
                    pl.col("date_first").alias("scheduled_transaction_first_date"),
                    pl.col("date_next").alias("scheduled_transaction_next_date"),
                    pl.col("frequency").alias("scheduled_transaction_frequency"),
                    pl.col("amount").alias("scheduled_transaction_amount"),
                    pl.col("memo").alias("scheduled_transaction_memo"),
                    pl.col("flag_color").alias("scheduled_transaction_flag_color"),
                    pl.col("account_id").alias("account_id"),
                    pl.col("payee_id").alias("payee_id"),
                    pl.col("category_id").alias("category_id"),
                    pl.col("transfer_account_id").alias("transfer_account_id"),
                ])
                .with_columns([
                    pl.col("memo").fill_null("unknown"),
                    (pl.col("amount") / 100).alias("scheduled_transaction_amount"),
                ])
                .drop([
                    "subtransactions", "deleted","flag_name","account_name",
                    "payee_name","category_name","ingestion_date"
                ])
            )
        except Exception as e:
            logging.error(f"Failed to transform the scheduled transactions DataFrame: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed scheduled transactions DataFrame to parquet file")
        try:
            scheduled_transactions_df.write_parquet(self.config['warehouse_data_path'] + '/scheduled_transactions.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed scheduled transactions DataFrame: {e}")

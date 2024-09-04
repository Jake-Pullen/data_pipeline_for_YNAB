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
        try:
            source_transactions = pl.read_parquet(self.file_path)
        except FileNotFoundError:
            logging.error("The transactions DataFrame does not exist")
            return

        try:
            base_transactions = source_transactions.select([
                "id",
                "date",
                "amount",
                "memo",
                "cleared",
                "approved",
                "flag_color",
                "account_id",
                "payee_id",
                "category_id",
                "transfer_account_id"
            ])
        except Exception as e:
            logging.error(f"Failed to select columns from the transactions DataFrame: {e}")
            return

        logging.info("Transforming the transactions DataFrame")
        try:
            resolve_transaction_dates = base_transactions.with_columns([
                pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d").alias("date")
            ])
        except Exception as e:
            logging.error(f"Failed to covert the date to date format: {e}")
            return

        try:
            add_transaction_prefix = resolve_transaction_dates.with_columns([
                pl.col("id").alias("transaction_id"),
                (pl.col("date").dt.year().cast(pl.Utf8) +
                    pl.col("date").dt.month().cast(pl.Utf8).str.zfill(2) +
                    pl.col("date").dt.day().cast(pl.Utf8).str.zfill(2)).alias("transaction_date"),
            ])
            fix_transaction_nulls = add_transaction_prefix.with_columns([
                pl.col("memo").fill_null("none"),
                pl.col("flag_color").fill_null("none"),
                pl.col("transfer_account_id").fill_null("none"),
                pl.col("category_id").fill_null("none"),
            ])
            fix_transaction_values = fix_transaction_nulls.with_columns([
                (pl.col("amount") / 1000).alias("transaction_amount")
            ])
            drop_transaction_columns = fix_transaction_values.drop([
                "id", "date", "amount"
            ])
            
        except Exception as e:
            logging.error(f"Failed to transform the transactions DataFrame: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed transactions DataFrame to parquet file")
        try:
            drop_transaction_columns.write_parquet(
                self.config['warehouse_data_path'] + '/transactions.parquet'
            )
        except Exception as e:
            logging.error(f"Failed to write the transformed transactions DataFrame: {e}")

class FactScheduledTransactions(Facts):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('scheduled_transactions.parquet')
        self.transform()

    def transform(self):
        try:
            source_scheduled = pl.read_parquet(self.file_path)
        except FileNotFoundError:
            logging.error("The scheduled transactions DataFrame does not exist")
            return

        try:
            base_scheduled = source_scheduled.select([
                "id",
                "date_first",
                "date_next",
                "frequency",
                "amount",
                "memo",
                "flag_color",
                "account_id",
                "payee_id",
                "category_id",
                "transfer_account_id"
            ])
        except Exception as e:
            logging.error(f"Failed to select columns from the scheduled transactions DataFrame: {e}")
            return

        try:
            resolve_scheduled_dates = base_scheduled.with_columns([
                pl.col("date_first").str.strptime(pl.Date, format="%Y-%m-%d").alias("date_first"),
                pl.col("date_next").str.strptime(pl.Date, format="%Y-%m-%d").alias("date_next")
            ])
        except Exception as e:
            logging.error(f"Failed to covert the date to date format: {e}")
            return
        
        logging.info("Transforming the scheduled transactions DataFrame")
        try:
            add_scheduled_prefix = resolve_scheduled_dates.with_columns([
                pl.col("id").alias("scheduled_transaction_id")
            ])
            fix_sheduled_nulls = add_scheduled_prefix.with_columns([
                pl.col("memo").fill_null("none"),
                pl.col("flag_color").fill_null("none"),
                pl.col("transfer_account_id").fill_null("none"),
                pl.col("category_id").fill_null("none"),
            ])
            fix_scheduled_values = fix_sheduled_nulls.with_columns([
                (pl.col("amount") / 1000).alias("scheduled_transaction_amount"),
            ])
            drop_scheduled_columns = fix_scheduled_values.drop([
                "id", "amount"
            ])
        except Exception as e:
            logging.error(f"Failed to transform the scheduled transactions DataFrame: {e}")
            return
        logging.info("Writing the transformed scheduled transactions DataFrame to parquet file")
        try:
            drop_scheduled_columns.write_parquet(self.config['warehouse_data_path'] + '/scheduled_transactions.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed scheduled transactions DataFrame: {e}")

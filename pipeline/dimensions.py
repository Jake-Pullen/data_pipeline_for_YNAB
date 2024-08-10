import polars as pl
import logging
import os
from datetime import date

class Dimensions:
    def __init__(self, config):
        self.config = config
        self.base_file_path = self.config['base_data_path']
        os.makedirs(self.config['warehouse_data_path'], exist_ok=True)
        
    def get_full_file_path(self, file_name):
        return f"{self.base_file_path}/{file_name}"
        
        
class DimAccounts(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('accounts.parquet')
        self.transform()

    def transform(self):
        # Read the parquet file into a polars DataFrame
        try:
            accounts_df = pl.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the base accounts parquet file: {e}")
            return

        # Transform the DataFrame
        logging.info("Transforming the accounts DataFrame")
        try:
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
        except Exception as e:
            logging.error(f"Failed to transform the accounts DataFrame: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed accounts DataFrame to parquet file")
        try:
            accounts_df.write_parquet(self.config['warehouse_data_path'] + '/accounts.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed accounts DataFrame to parquet file: {e}")
            return

class DimCategories(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('categories.parquet')
        self.transform()
    
    def transform(self):
        # Read the parquet file into a polars DataFrame
        try:
            categories_df = pl.read_parquet(self.file_path)
        except Exception as e: 
            logging.error(f"Failed to read the base categories parquet file: {e}")
            return
        logging.info("Transforming the categories DataFrame")
        try:
            categories_df = categories_df.select([
                'id',
                'name',
                'category_group_name',
                'hidden',
                'note',
                'budgeted',
                'activity',
                'balance',
                'deleted'
            ])
        except Exception as e:
            logging.error(f"Failed to select columns from the categories DataFrame: {e}")
            return
        
        try:
            # Rename the columns
            categories_df = categories_df.with_columns(pl.col('id').alias('category_id'))
            categories_df = categories_df.with_columns(pl.col('name').alias('category_name'))

            # Fill null values in the note column
            categories_df = categories_df.with_columns(pl.col('note').fill_null('unknown'))

            # Convert the balance, budgeted, and activity columns to decimal
            categories_df = categories_df.with_columns(pl.col('balance') / 100)
            categories_df = categories_df.with_columns(pl.col('budgeted') / 100)
            categories_df = categories_df.with_columns(pl.col('activity') / 100)
        except Exception as e:
            logging.error(f"Failed to transform the categories DataFrame: {e}")
            return

        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed categories DataFrame to parquet file")
        try:
            categories_df.write_parquet(self.config['warehouse_data_path'] + '/categories.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed categories DataFrame to parquet file: {e}")
            return
        
class DimPayees(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('payees.parquet')
        self.transform()
    
    def transform(self):
        # Read the parquet file into a polars DataFrame
        try:
            payees_df = pl.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the base payees parquet file: {e}")
            return
        logging.info("Transforming the payees DataFrame")
        try:
            payees_df = payees_df.select([
                'id',
                'name',
                'deleted'
            ])
        except Exception as e:
            logging.error(f"Failed to select columns from the payees DataFrame: {e}")
            return
        try:
            # Rename the columns
            payees_df = payees_df.with_columns(pl.col('id').alias('payee_id'))
            payees_df = payees_df.with_columns(pl.col('name').alias('payee_name'))
        except Exception as e:
            logging.error(f"Failed to rename columns in the payees DataFrame: {e}")
            return

        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed payees DataFrame to parquet file")
        try:
            payees_df.write_parquet(self.config['warehouse_data_path'] + '/payees.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed payees DataFrame to parquet file: {e}")
            return

class DimDate(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.transform()
    
    def transform(self):
        # Create a DataFrame with dates from 2020-01-01 to 2030-12-31
        try:
            dates_df = pl.DataFrame({'date':pl.date_range(date(2020, 1, 1), date(2030, 12, 31), "1d", eager=True)})
        except Exception as e:
            logging.error(f"Failed to create a DataFrame with dates: {e}")
            return
        # Extract year, month, day, and weekday from the date column
        try:
            dates_df = dates_df.with_columns([
                pl.col('date').dt.year().alias('year'),
                pl.col('date').dt.month().alias('month'),
                pl.col('date').dt.day().alias('day'),
                pl.col('date').dt.weekday().alias('weekday')
            ])
        except Exception as e:
            logging.error(f"Failed to extract year, month, day, and weekday from the date column: {e}")
            return 
        try:
            # Create a new column to indicate if the date is a weekday or weekend
            dates_df = dates_df.with_columns([
                (pl.col('weekday') < 5).alias('is_weekday')  # True for weekdays (Monday to Friday), False for weekends (Saturday and Sunday)
            ])
        except Exception as e:
            logging.error(f"Failed to create a new column to indicate if the date is a weekday or weekend: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed dates DataFrame to parquet file")
        try:
            dates_df.write_parquet(self.config['warehouse_data_path'] + '/dates.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed dates DataFrame to parquet file: {e}")
            return


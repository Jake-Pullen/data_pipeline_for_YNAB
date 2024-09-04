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
            source_accounts = pl.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the base accounts parquet file: {e}")
            return

        logging.info("Transforming the accounts DataFrame")
        try:
            base_accounts = (
                source_accounts.select([
                    "id",
                    "name",
                    "type",
                    "on_budget",
                    "closed",
                    "note",
                    "balance",
                    "cleared_balance",
                    "uncleared_balance",
                    "deleted"
                ])
            )
        except Exception as e:
            logging.error(f"Failed to select columns from the categories DataFrame: {e}")
            return

        try:
            add_accounts_prefix = base_accounts.with_columns([
                pl.col("id").alias("account_id"),
                pl.col("name").alias("account_name"),
                pl.col("type").alias("account_type")
            ])
            fill_accounts_null_values = add_accounts_prefix.with_columns([
                pl.col('note').fill_null('none')
            ])
            fix_accounts_values = fill_accounts_null_values.with_columns([
                (pl.col("balance") / 1000).alias("balance"),
                (pl.col("cleared_balance") / 1000).alias("cleared_balance"),
                (pl.col("uncleared_balance") / 1000).alias("uncleared_balance"),
            ])
            drop_accounts_columns = fix_accounts_values.drop([
                "id", "name", "type"
            ])
        except Exception as e:
            logging.error(f"Failed to transform the accounts DataFrame: {e}")
            return

        logging.info("Writing the transformed accounts DataFrame to parquet file")
        try:
            drop_accounts_columns.write_parquet(self.config['warehouse_data_path'] + '/accounts.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed accounts DataFrame to parquet file: {e}")
            return

class DimCategories(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('categories.parquet')
        self.transform()
    
    def transform(self):
        try:
            source_categories = pl.read_parquet(self.file_path)
        except Exception as e: 
            logging.error(f"Failed to read the base categories parquet file: {e}")
            return
        logging.info("Transforming the categories DataFrame")
        try:
            base_categories = source_categories.select([
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
            add_categories_prefix = base_categories.with_columns([
                pl.col('id').alias('category_id'),
                pl.col('name').alias('category_name')
            ])
            fill_null_category_values = add_categories_prefix.with_columns([
                pl.col('note').fill_null('none')
            ])
            fix_categories_values = fill_null_category_values.with_columns([
                (pl.col('balance') / 1000),
                (pl.col('budgeted') / 1000),
                (pl.col('activity') / 1000)
            ])
            drop_categories_columns = fix_categories_values.drop([
                'id', 'name'
            ])
        except Exception as e:
            logging.error(f"Failed to transform the categories DataFrame: {e}")
            return

        logging.info("Writing the transformed categories DataFrame to parquet file")
        try:
            drop_categories_columns.write_parquet(self.config['warehouse_data_path'] + '/categories.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed categories DataFrame to parquet file: {e}")
            return

class DimPayees(Dimensions):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.get_full_file_path('payees.parquet')
        self.transform()
    
    def transform(self):
        try:
            source_payees = pl.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the base payees parquet file: {e}")
            return
        logging.info("Transforming the payees DataFrame")
        try:
            base_payees = source_payees.select([
                'id',
                'name',
                'deleted'
            ])
        except Exception as e:
            logging.error(f"Failed to select columns from the payees DataFrame: {e}")
            return

        try:
            add_payees_prefix = base_payees.with_columns([
                pl.col('id').alias('payee_id'),
                pl.col('name').alias('payee_name')
            ])
            drop_payees_columns = add_payees_prefix.drop([
                'id', 'name'
            ])
        except Exception as e:
            logging.error(f"Failed to rename columns in the payees DataFrame: {e}")
            return

        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed payees DataFrame to parquet file")
        try:
            drop_payees_columns.write_parquet(self.config['warehouse_data_path'] + '/payees.parquet')
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
                (pl.col('weekday') < 6).alias('is_weekday')  # True for weekdays (Monday to Friday), False for weekends (Saturday and Sunday)
            ])
        except Exception as e:
            logging.error(f"Failed to create a new column to indicate if the date is a weekday or weekend: {e}")
            return
        
        # Create a primary key by concatenating year, month, and day with no separators
        try:
            dates_df = dates_df.with_columns([
                (pl.col('year').cast(pl.Utf8) + 
                 pl.col('month').cast(pl.Utf8).str.zfill(2) +
                 pl.col('day').cast(pl.Utf8).str.zfill(2)
                ).alias('date_id')
            ])
        except Exception as e:
            logging.error(f"Failed to create the primary key column: {e}")
            return
        # Write the DataFrame to a new parquet file
        logging.info("Writing the transformed dates DataFrame to parquet file")
        try:
            dates_df.write_parquet(self.config['warehouse_data_path'] + '/dates.parquet')
        except Exception as e:
            logging.error(f"Failed to write the transformed dates DataFrame to parquet file: {e}")
            return


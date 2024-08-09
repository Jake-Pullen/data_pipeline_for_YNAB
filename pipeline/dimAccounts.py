# This file is used to define the dimension table for the accounts table
# The accounts table contains information about the accounts in the budget
# The accounts table has the following columns:
# - id: the unique identifier for the account
# - name: the name of the account
# - type: the type of the account (e.g. checking, savings, credit card)
# - on_budget: a boolean indicating whether the account is on budget
# - closed: a boolean indicating whether the account is closed
# - note: a note associated with the account
# - balance: the current balance of the account
# - cleared_balance: the cleared balance of the account
# - uncleared_balance: the uncleared balance of the account
# - deleted: a boolean indicating whether the account has been deleted


# the below is mega tbc

import pandas as pd
from datetime import datetime

def handle_scd_type_2(dim_accounts_df, new_data_df):
    current_date = datetime.now().date()
    
    for index, new_row in new_data_df.iterrows():
        account_id = new_row['account_id']
        existing_rows = dim_accounts_df[dim_accounts_df['account_id'] == account_id]
        
        if existing_rows.empty:
            # Insert new record
            new_row['start_date'] = current_date
            new_row['end_date'] = None
            new_row['is_current'] = True
            dim_accounts_df = dim_accounts_df.append(new_row, ignore_index=True)
        else:
            current_row = existing_rows[existing_rows['is_current'] == True].iloc[0]
            if not new_row.equals(current_row.drop(['surrogate_key', 'start_date', 'end_date', 'is_current'])):
                # Update existing record to set is_current to False and end_date
                dim_accounts_df.loc[current_row.name, 'is_current'] = False
                dim_accounts_df.loc[current_row.name, 'end_date'] = current_date
                
                # Insert new record
                new_row['start_date'] = current_date
                new_row['end_date'] = None
                new_row['is_current'] = True
                dim_accounts_df = dim_accounts_df.append(new_row, ignore_index=True)
    
    return dim_accounts_df

# Example usage
dim_accounts_df = pd.DataFrame(columns=[
    'surrogate_key', 'account_id', 'account_name', 'account_type', 'on_budget', 'closed', 'note', 
    'balance', 'cleared_balance', 'uncleared_balance', 'deleted', 'start_date', 'end_date', 'is_current'
])

new_data_df = pd.DataFrame([
    {'account_id': 1, 'account_name': 'Checking Account', 'account_type': 'checking', 'on_budget': True, 'closed': False, 'note': '', 'balance': 1000.00, 'cleared_balance': 1000.00, 'uncleared_balance': 0.00, 'deleted': False},
    {'account_id': 2, 'account_name': 'Savings Account', 'account_type': 'savings', 'on_budget': True, 'closed': False, 'note': '', 'balance': 5000.00, 'cleared_balance': 5000.00, 'uncleared_balance': 0.00, 'deleted': False}
])

dim_accounts_df = handle_scd_type_2(dim_accounts_df, new_data_df)
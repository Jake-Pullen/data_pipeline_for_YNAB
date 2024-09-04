'''Module to create a Dash app that displays visualizations of YNAB data.'''

import polars as pl
import plotly.express as px
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
import logging
import sys
import config.exit_codes as ec

try:
    accounts = pl.read_parquet('data/warehouse/accounts.parquet')
    categories = pl.read_parquet('data/warehouse/categories.parquet')
    dates = pl.read_parquet('data/warehouse/dates.parquet')
    payees = pl.read_parquet('data/warehouse/payees.parquet')
    scheduled_transactions = pl.read_parquet('data/warehouse/scheduled_transactions.parquet')
    transactions = pl.read_parquet('data/warehouse/transactions.parquet')
except FileNotFoundError:
    logging.error('Data warehouse files not found. Run the data pipeline to create them.')
    sys.exit(ec.MISSING_DATA_FILES)

try:
    # Join transactions with accounts, categories, and payees to create a master DataFrame
    master_transactions = transactions.join(categories, left_on='category_id', right_on='category_id', suffix='_category')\
                        .join(accounts, left_on='account_id', right_on='account_id', suffix='_account')\
                        .join(payees, left_on='payee_id', right_on='payee_id', suffix='_payee')\
                        .join(dates, left_on='transaction_date', right_on='date_id', suffix='_date')
except Exception as e:
    logging.error(f'Error joining DataFrames: {e}')
    sys.exit(ec.BAD_JOIN)

# Create aggregations
spend_per_day = master_transactions.sql('''
    SELECT 
        date,
        year,
        month,
        day,
        ABS(SUM(transaction_amount)) as total
    FROM self
    WHERE category_name != 'Inflow: Ready to Assign'
    GROUP BY date, year, month, day
    ORDER BY date DESC
    '''
)

spend_per_category = master_transactions.sql('''
    SELECT
        category_name,
        ABS(SUM(transaction_amount)) as total
    FROM self
    WHERE category_name != 'Inflow: Ready to Assign'
    GROUP BY category_name
    ORDER BY total DESC
    '''
)

spend_per_payee = master_transactions.sql('''
    SELECT
        payee_name,
        ABS(SUM(transaction_amount)) as total
    FROM self
    WHERE payee_name != 'Starting Balance'
        AND transaction_amount < 0
    GROUP BY payee_name
    ORDER BY total DESC
    '''
)

# Convert DataFrame to list of dictionaries
spend_per_day_data = spend_per_day.to_dicts()
spend_per_category_data = spend_per_category.to_dicts()
spend_per_payee_data = spend_per_payee.to_dicts()

# Convert list of dictionaries to Pandas DataFrame
spend_per_day_df = pd.DataFrame(spend_per_day_data)
spend_per_category_df = pd.DataFrame(spend_per_category_data)
spend_per_payee_df = pd.DataFrame(spend_per_payee_data)

spend_per_day_line = px.line(spend_per_day_df, x="date", y="total")
spend_per_day_line.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font_color='white'
)

spend_per_category_bar = px.bar(spend_per_category_df, x="category_name", y="total")
spend_per_category_bar.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font_color='white'
)

spend_per_payee_bar = px.bar(spend_per_payee_df, x="payee_name", y="total")
spend_per_payee_bar.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font_color='white'
)

# Initialize the app with a dark theme
app = Dash(external_stylesheets=[dbc.themes.DARKLY])

# App layout
app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.Div("Data Pipeline For YNAB, Preview Visualisations",
                        className="text-center text-light"),
                        width=12
                )
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("Spend Per Day", className="card-title"),
                                dcc.Graph(figure=spend_per_day_line)
                            ]
                        ),
                        className="mb-4"
                    ),
                    width=12
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("Spend Per Category", className="card-title"),
                                dcc.Graph(figure=spend_per_category_bar)
                            ]
                        ),
                        className="mb-4"
                    ),
                    width=6
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("Spend Per Payee", className="card-title"),
                                dcc.Graph(figure=spend_per_payee_bar)
                            ]
                        ),
                        className="mb-4"
                    ),
                    width=6
                )
            ]
        )
    ],
    fluid=True
)

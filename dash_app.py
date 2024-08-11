import polars as pl
import plotly.express as px
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dash_table import DataTable

# Incorporate data
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

# Convert DataFrame to list of dictionaries
data = relevant_data.to_dicts()
print("Data converted to list of dictionaries:")
print(data)

# Initialize the app with a dark theme
app = Dash(external_stylesheets=[dbc.themes.DARKLY])

# Create the line graph with dark mode styling
fig = px.line(relevant_data.to_pandas(), x="date", y="total", title='Spend Per Day')
fig.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font_color='white'
)

# App layout
app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(html.Div("My First App with My Data", className="text-center text-light"), width=12)
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("Data Table", className="card-title"),
                                DataTable(
                                    data=data, 
                                    columns=[{"name": i, "id": i} for i in relevant_data.columns], 
                                    page_size=5,
                                    style_header={'backgroundColor': 'black', 'color': 'white'},
                                    style_cell={'backgroundColor': 'black', 'color': 'white'}
                                )
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
                                html.H4("Spend Per Day", className="card-title"),
                                dcc.Graph(figure=fig)
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

# Run the app
if __name__ == '__main__':
    app.run(debug=True)
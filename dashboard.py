import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from os import listdir

app = dash.Dash()

def open_csv(filename):
    df = pd.read_csv(filename, header=None)
    return df

def create_fig():
    path = './my_state_counter.csv/'
    files = [path+_ for _ in listdir(path) if _.endswith('csv')]
    df = pd.concat(map(open_csv,files))
    df.columns = ['state','amount']
    df = df.sort_values(by=['amount'])
    data = go.Bar(x=df['state'],y=df['amount'])
    layout = go.Layout(title='Total Sales by State',xaxis={"title":"States"},yaxis={"title":"Amount"})
    fig = go.Figure(data=data, layout=layout)
    return fig


app.layout = html.Div([
                dcc.Graph(id='sale_by_state', figure=create_fig()),
                dcc.Interval(id='update-interval',interval=5000,n_intervals=0)
])

@app.callback(Output('sale_by_state','figure'),
             [Input('update-interval','n_intervals')])
def update_graph(n):
    try:
        return create_fig()
    except:
        return None



if __name__ == '__main__':
    app.run_server(port=9001)

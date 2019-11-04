import dash 
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd 
import plotly as py
import plotly.graph_objs as go
import sqlite3
import csv

DEFAULT_LAYOUT = go.Layout(
                xaxis = go.layout.XAxis(
                    showgrid = False,
                    showline = False,
                    zeroline = False,
                    showticklabels=False

                ),
                yaxis = go.layout.YAxis(
                    showgrid = False,
                    showline = False,
                    zeroline = False,
                    showticklabels=False
   
                )
            )


app = dash.Dash(__name__)
server = app.server
app.title = 'AirBnB DataVi'

global state 
state = {
    "source": " ",
    "number": " ",
    "review": " "
}

    
def build_selection_sources_right():

    conn = sqlite3.connect('AirBnB.db')
    c = conn.cursor()
    sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
    c.execute(sql)
    l = []
    for elt in c.fetchall():
        l.append({'label': str(elt[0]), 'value': str(elt[0])})
    return l


def build_selection_ids(source):

    if source is None:
        return []
    else:
        conn = sqlite3.connect('AirBnB.db')
        c = conn.cursor()
        c.execute("select distinct listing_id from "+ str(source)+" order by listing_id ASC")
        l = []
        for elt in c.fetchall():
            l.append({'label': str(elt[0]), 'value': str(elt[0])})
        
        return l


def get_tables_calendars():

    conn = sqlite3.connect('AirBnB.db')
    c = conn.cursor()
    sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
    c.execute(sql)

get_tables_calendars()



def build_histo(source, num):
    if source is None and num is None: 
        return {
            'data': [
                go.Bar(
                    x = [],
                    y = [],
                    marker = {
                        'color': 'cornflowerblue'   
                    },
                    visible = False
                )
            ],
            'layout' : DEFAULT_LAYOUT
        }
    else:
        conn = sqlite3.connect('AirBnB.db')
        c = conn.cursor()
        sql = "SELECT date, available FROM " + str(source) + " WHERE listing_id=" + str(num)
        c.execute(sql)
        res = c.fetchall()
        dates = [x[0] for x in res]
        avail = [y[1] for y in res]
        available = []
        for elt in avail:
            if elt == 't':
                available.append(1)
            if elt == 'f':
                available.append(0)
        return {
                'data': [
                    go.Bar(
                        x = dates,
                        y = available,
                        marker = {
                            'color': 'cornflowerblue'  
                        },
                    )
                ],
                'layout' : go.Layout(
                title = 'Histogramme de disponibilité',
                margin = go.layout.Margin(
                    r = 180,
                    l = 180,
                    pad = 10,
                ),
                xaxis = go.layout.XAxis(
                    showgrid = False,
                    showline = False,
                    zeroline = False,
                ),
                yaxis = go.layout.YAxis(
                    showgrid = False,
                    showline = False,
                    zeroline = False,
            
                )
            )
                    
            }

def build_pie_booking(source, num):
    if source is None and num is None: 
        return {
            'data': [
                go.Pie(
                    labels = [],
                    values = [],
                    hole = 0.5,
                )
            ],
            'layout' : DEFAULT_LAYOUT
        }
    else:
        conn = sqlite3.connect('AirBnB.db')
        c = conn.cursor()
        sql = "SELECT available FROM " + str(source) + " WHERE listing_id=" + str(num)
        c.execute(sql)
        res = c.fetchall()
        x = ['jours disponibles', 'jours non disponibles']
        t = 0
        f = 0
        avail = [y[0] for y in res]
        for elt in avail:
            if elt == 't':
                t = t + 1
            if elt == 'f':
                f = f + 1
        y = [t, f]

        return {
            'data': [
                go.Pie(
                    labels = x,
                    values = y,
                    hole = 0.5
                )
            ],
             'layout' : go.Layout(
                title = "Parts des jours disponibles et non disponibles sur l'année"
            )
        }

#def build_scatter_comments()

default_x = None
default_y = None

app.layout = html.Div([
    html.P('Visualisation de données - AirBnB', style = {'color':'steelblue', 'textAlign':'center', 'fontWeight':'bold'}),
    html.Div([
        html.Div([
            html.P("Informations globales sur le calendrier", style = {'color':'darkgray'}),
            html.P("Sélectionnez un fichier source:", style = {'color':'lightgray', 'textAlign':'center'}),
            dcc.Dropdown(
                id = 'list_sources',
                style = {'width':'250px', 'backgroundColor':'lightgray', 'marginLeft':'auto', 'marginRight':'auto'},
                options = build_selection_sources_right(),
                placeholder="All",
                multi = False
            ),
            html.P(),
            html.Div([
                html.Button('Appliquer', id='button_sources', style = {'width':'90px', 'height':'30px', 'backgroundColor':'gray', 'border':'none', 'color':'white', 'cursor':'pointer'}),

            ], style = {'textAlign':'center'}),
            html.Div(
                id = 'div1',
                    children = [
                        html.Div(
                            id = 'graph1_container',
                            children = [
                                dcc.Graph(
                                    id = 'graph_histo_avg',
                                    figure = build_histo(default_x, default_y)
                                    
                                )
                            ]
                        ),
                    ]
            ),    
        ], id = "left"),
        html.Div([
            html.P("Informations complémentaires sur les annonces", style = {'color':'darkgray'}),

            html.P("Sélectionnez un fichier source:", style = {'color':'lightgray', 'textAlign':'center'}),
            dcc.Dropdown(
                id = 'list_sources_right',
                style = {'width':'250px', 'backgroundColor':'lightgray', 'marginLeft':'auto', 'marginRight':'auto'},
                options = build_selection_sources_right(),
                placeholder="All",
                multi = False
            ),
            html.P(),
            html.Div([
                html.Button('Appliquer', id='apply_source', style = {'width':'90px', 'height':'30px', 'backgroundColor':'gray', 'border':'none', 'color':'white', 'cursor':'pointer'}),

            ], style = {'textAlign':'center'}),

            html.P("Sélectionnez un numéro d'annonce:", style = {'color':'lightgray', 'textAlign':'center'}),
            dcc.Dropdown(
                id = 'list_ids',
                style = {'width':'250px', 'backgroundColor':'lightgray', 'marginLeft':'auto', 'marginRight':'auto'},
                options = build_selection_ids(None),
                placeholder="All",
                multi = False
            ),
            html.P(),
            html.Div([
                html.Button('Appliquer', id='button_ids_right', style = {'width':'90px', 'height':'30px', 'backgroundColor':'gray', 'border':'none', 'color':'white', 'cursor':'pointer'}),

            ], style = {'textAlign':'center'}),

            html.Div(
                id = 'div2',
                    children = [
                        html.Div(
                            id = 'graph2_container',
                            children = [
                                dcc.Graph(
                                    id = 'graph_histo',
                                    figure = build_histo(default_x, default_y)
                                    
                                )
                            ]
                        ),
                    ]
            ),   
            html.Div(
                id = 'div3',
                    children = [
                        html.Div(
                            id = 'graph3_container',
                            children = [
                                dcc.Graph(
                                    id = 'pie_booking',
                                    figure = build_pie_booking(default_x, default_y)
                                    
                                )
                            ]
                        ),
                    ]
            ),   

            html.P("Sélectionnez un fichier reviews:", style = {'color':'lightgray', 'textAlign':'center'}),
            dcc.Dropdown(
                id = 'list_reviews',
                style = {'width':'250px', 'backgroundColor':'lightgray', 'marginLeft':'auto', 'marginRight':'auto'},
                options = build_selection_sources_right(),
                placeholder="All",
                multi = False
            ),
            html.P(),
            html.Div([
                html.Button('Appliquer', id='button_reviews_right', style = {'width':'90px', 'height':'30px', 'backgroundColor':'gray', 'border':'none', 'color':'white', 'cursor':'pointer'}),

            ], style = {'textAlign':'center'}),

            html.P("Sélectionnez un numéro d'annonce:", style = {'color':'lightgray', 'textAlign':'center'}),
            dcc.Dropdown(
            id = 'list2_ids',
            style = {'width':'250px', 'backgroundColor':'lightgray', 'marginLeft':'auto', 'marginRight':'auto'},
            options = build_selection_ids(None),
            placeholder="All",
            multi = False
            ),
            html.P(),
            html.Div([
                html.Button('Appliquer', id='apply_reviews_right', style = {'width':'90px', 'height':'30px', 'backgroundColor':'gray', 'border':'none', 'color':'white', 'cursor':'pointer'}),

            ], style = {'textAlign':'center'}),

            ], id = "right"),

    ], id = "main_page"),
   
    

])
'''
@app.callback(
    Output('graph_histo_avg', 'figure'),
    [Input('button_sources', 'n_clicks')],
    [State('list_sources', 'value')],
)
def update_left(n_clicks, source):

    state["source"] = source
    conn = sqlite3.connect('AirBnB.db')
    c = conn.cursor()
    sql = "SELECT listing_id, available FROM" + str(state["source"]) 
    c.execute(sql)
    res = c.fetchall()
    listing_id = [x[0] for x in res]
    avail = [y[1] for y in res]
    

    #return build_selection_ids(source)
'''
@app.callback(
    Output('list_ids', 'options'),
    [Input('apply_source', 'n_clicks')],
    [State('list_sources_right', 'value')]
)
def update_ids_right(n_clicks, source):
    state["source"] = source
    l = build_selection_ids(state["source"])
    return l

@app.callback(
    Output('list2_ids', 'options'),
    [Input('button_reviews_right', 'n_clicks')],
    [State('list_reviews', 'value')]
)
def update_ids_from_reviews(n_clicks, reviews):
    state["review"] = reviews
    l = build_selection_ids(state["review"])
    return l


@app.callback(
    [Output('graph_histo', 'figure'), Output('pie_booking', 'figure')],
    [Input('button_ids_right', 'n_clicks')],
    [State('list_sources_right', 'value'),State('list_ids', 'value')],
)
def uptdate_histo_pie_right(n_clicks, source, num):

    state["num"] = num
    state["source"] = source

    if state["source"] is None and num is None:
        return build_histo(None, None), build_pie_booking(None, None)
    else:
        
        return build_histo(str(state["source"]), str(num)), build_pie_booking(str(state["source"]), str(num))



if __name__ == '__main__':
    app.run_server(debug=True)
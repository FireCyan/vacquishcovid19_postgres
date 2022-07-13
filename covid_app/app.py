# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 11:42:36 2021

@author: john.yang
"""
###############################################################################
# Initial imports

import os, sys, re 
from pathlib import Path 
import pandas as pd
import numpy as np


import dash
import dash_bootstrap_components as dbc
import dash_html_components as html

if os.environ.get("USERNAME") == 'john':
    from jupyter_dash import JupyterDash

# from covid_app.layout_main import layout
from layout_main import layout
from callbacks import register_callbacks

###############################################################################
# Start coding

# For the structure of the app, I took Phillippe's advice from this thread:
# https://community.plotly.com/t/dash-callback-in-a-separate-file/14122/15

##### Use Dash if running the script in Anaconda prompt #####
##### Use JupyterDash if running in Spyder (or Jupyter Notebook) #####

shell = ''
try:
    shell = get_ipython().__class__.__name__
    if shell == 'ZMQInteractiveShell':
        app = JupyterDash(__name__, external_stylesheets =[dbc.themes.BOOTSTRAP])
    elif shell == 'TerminalInteractiveShell':
        app = dash.Dash(__name__)
        # , external_stylesheets =[dbc.themes.BOOTSTRAP]
    else:        
        print('Not shell')
        app = dash.Dash(__name__, external_stylesheets =[dbc.themes.BOOTSTRAP], url_base_pathname='/')
except NameError:
    app = dash.Dash(__name__, external_stylesheets =[dbc.themes.BOOTSTRAP])
        
app.layout = layout
# app.layout = html.Div([layout])
register_callbacks(app)

app.config.suppress_callback_exceptions = True




if __name__ == '__main__':
    if os.environ.get("USERNAME") == 'john':
        # app.run_server(host="127.0.0.1", debug=False, port=int(os.environ['CDSW_APP_PORT']))
        # app.run_server(host="127.0.0.1", debug=False, port=int(os.environ['CDSW_APP_PORT']))
        # app.run_server(host="0.0.0.0", debug=False, port=int(os.environ['CDSW_APP_PORT']))
        app.run_server(host="127.0.0.1", port='8052', debug=True)
        # app.run_server(debug=True)
    else:
        ##### Only do Google Analytics if hosted online #####
        # app.scripts.config.serve_locally = False
        # app.scripts.append_script({
        # "external_url": "https://www.googletagmanager.com/gtag/js?id=G-XS013W81ZD"
        # })

        # app.scripts.append_script({
        # 'external_url': 'http://www.vacquishcovid19.com/assets/gtag.js'
        # })
        
        app.run_server(port='8052', host="0.0.0.0", debug=False)
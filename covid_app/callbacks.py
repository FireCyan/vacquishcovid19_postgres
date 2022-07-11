# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 00:11:07 2021

@author: john.yang
"""

###############################################################################
# Initial imports
import os, sys, re 
from pathlib import Path 
import pandas as pd
import numpy as np
import traceback

from dash.dependencies import Output, Input, State
import dash
import dash_core_components as dcc
# https://community.plotly.com/t/its-possible-to-raise-prevent-update-for-only-one-callback-output/30973
from dash.dash import no_update
from dash.exceptions import PreventUpdate
import plotly
import plotly.graph_objects as go
import urllib


import re

# https://stackoverflow.com/questions/595305/how-do-i-get-the-path-of-the-python-script-i-am-running-in
current_file_path = Path(os.path.realpath(__file__))

# https://stackoverflow.com/questions/2860153/how-do-i-get-the-parent-directory-in-python
current_wd = current_file_path.parent.parent.absolute()

# current_wd = Path(r'C:\John_folder\Current_focus\0_host_covid_19_plots_project\covid-19_vaccination_postgres')

modules=[current_wd]

for module in modules: 
    for fld in module.glob('**'): 
        if re.search('__pycache__', str(fld)) is None and re.search('\.git', str(fld)) is None and str(fld) not in sys.path: 
            sys.path.append(str(fld))


from jupyter_dash import JupyterDash
import plotly.express as px
import plotly.tools as tls
import plotly.graph_objects as go

# custom modules
import query_processing as qp
import covid_plot as cp
###############################################################################


df_case_loc_vac = qp.combine_case_vac_lookup()
df_case_vac, df_curr_case_vac = qp.get_adjusted_people_vaccinated(df_case_loc_vac)


def register_callbacks(app):
    # Callbacks

    # [Output('abs_case_vac_latest', 'figure'),
    #     Output('per_100k_case_vac_latest', 'figure')],
    @app.callback(
        Output('case_vac_latest', 'figure'),
        [Input('country_dropdown_latest', 'value'),
        Input('continent_dropdown_latest', 'value'),
        Input('plot_source_latest', 'value'),
        Input('vac_value_type', 'value'),
        Input('x_axis_log', 'value'),
        Input('case_value_type', 'value'),
        Input('y_axis_log', 'value')]
    )
    def update_case_vac_latest_figure(selected_country, selected_continent, use_continent_or_country, x_type, x_scale, y_type, y_scale):
        if selected_country is not None:
            if len(selected_country) == 0:
                selected_country = 'all'

        # Determine to use Continent or country/region dropdown for plotting latest status
        if use_continent_or_country == 'Continent':
            # If use continent, then no restriction on country/region (i.e., all countries/regions)
            selected_country = 'all'
        else:
            # If use country/region, then no restriction on continent (i.e., all continents)
            selected_continent = list(df_case_loc_vac['continent'].dropna().unique())

        
        # x value to use and x-axis scale
        if x_type == 'Percentage':
            # x_col = 'percent_adjusted_people_vaccinated'
            x_col = 'percent_total_vac_over_population'
        else:
            # x_col = 'adjusted_people_vaccinated'
            x_col = 'total_vaccinations'
        
        xlog = 'log' if x_scale == 'Log scale' else 'linear'

        # y value to use and y-axis scale
        if y_type == 'Per 100K':
            y_col = 'past_week_daily_cases_per_100k'
        else:
            y_col = 'past_week_daily_cases'
        
        ylog = 'log' if y_scale == 'Log scale' else 'linear'

        fig_case_vac_latest = cp.plot_latest_case_vac(df_curr_case_vac, x_col=x_col, xlog=xlog, y_col=y_col, ylog=ylog, countries=selected_country, continents=selected_continent)

        # fig_abs_case_vac_latest = cp.plot_latest_case_vac(df_curr_case_vac, x_col=x_col, xlog=xlog, y_col='past_week_daily_cases', ylog=ylog, countries=selected_country, continents=selected_continent)
        # fig_per_100k_case_vac_latest = cp.plot_latest_case_vac(df_curr_case_vac, x_col=x_col, xlog=xlog, y_col='past_week_daily_cases_per_100k', ylog=ylog, countries=selected_country, continents=selected_continent)

        return fig_case_vac_latest


    # [Output('abs_case_vac_time_series', 'figure'),
    #     Output('per_100k_case_vac_time_series', 'figure')],
    @app.callback(
        Output('case_vac_time_series', 'figure'),
        [Input('country_dropdown_all', 'value'),
        Input('vac_value_type', 'value'),
        Input('x_axis_log', 'value'),
        Input('case_value_type', 'value'),
        Input('y_axis_log', 'value')]
    )
    def update_case_vac_all_available_figure(selected_country, x_type, x_scale, y_type, y_scale):

        # x value to use and x-axis scale
        if x_type == 'Percentage':
            # x_col = 'percent_adjusted_people_vaccinated'
            x_col = 'percent_total_vac_over_population'
        else:
            # x_col = 'adjusted_people_vaccinated'
            x_col = 'total_vaccinations'
        
        xlog = 'log' if x_scale == 'Log scale' else 'linear'

        # y value to use and y-axis scale
        if y_type == 'Per 100K':
            y_col = 'past_week_daily_cases_per_100k'
        else:
            y_col = 'past_week_daily_cases'
        
        ylog = 'log' if y_scale == 'Log scale' else 'linear'

        fig_case_vac_all_available = cp.plot_case_vac_ts(df_case_vac, x_col=x_col, xlog=xlog, y_col=y_col, ylog=ylog, country=selected_country)
                
        # fig_abs_case_vac_all_available = cp.plot_case_vac_ts(df_case_vac, country=selected_country, y_col='past_week_daily_cases')
        # fig_per_100k_case_vac_all_available = cp.plot_case_vac_ts(df_case_vac, country=selected_country, y_col='past_week_daily_cases_per_100k')

        return fig_case_vac_all_available


        # Input('per_100k_case_vac_latest', 'clickData')
        # prop_clickdata
    @app.callback(
        Output('country_dropdown_all', 'value'),
        [Input('case_vac_latest', 'clickData')],
        [State('country_dropdown_all', 'value')]
    )

    def update_all_available_data_dropdown(clickdata, countries_present):
        added_countries = []
        if clickdata is not None:
            added_countries.append(clickdata['points'][0]['text'])
        
        # if prop_clickdata is not None:
        #     added_countries.append(prop_clickdata['points'][0]['text'])

        if countries_present is not None:
            if len(countries_present) != 0:
                if isinstance(countries_present, list):
                    all_countries = countries_present + added_countries
                else:
                    all_countries = [countries_present] + added_countries
            else:
                all_countries = added_countries

        print(all_countries)

        return all_countries

    @app.callback(
        Output('country_dropdown_latest', 'value'),
        [Input('fill_cr', 'n_clicks')],
        [State('continent_dropdown_latest', 'value'),
        State('country_dropdown_latest', 'value')]
    )

    def fill_country_region(fill_click, list_continents, list_country_region):
        if (fill_click > 0):
            mat_continent = df_curr_case_vac['continent'].isin(list_continents)
            return list(df_curr_case_vac[mat_continent]['country_region'].unique())
        else:
            return list_country_region

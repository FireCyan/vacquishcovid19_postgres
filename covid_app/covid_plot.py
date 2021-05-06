# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 23:28:07 2021

@author: john.yang

Plotting functions
"""

###############################################################################
# Initial imports
import os, sys, re 
from pathlib import Path

current_wd = Path(r'C:\John_folder\Current_focus\0_host_covid_19_plots_project\covid-19_vaccination_postgres')

modules=[current_wd]

for module in modules: 
    for fld in module.glob('**'): 
        if re.search('__pycache__', str(fld)) is None and re.search('\.git', str(fld)) is None and str(fld) not in sys.path: 
            sys.path.append(str(fld))


# from jupyter_dash import JupyterDash
import plotly.express as px
import plotly.tools as tls
import plotly.graph_objects as go

###############################################################################


def plot_latest_case_vac(df, x_col='percent_adjusted_people_vaccinated', y_col='past_week_daily_cases', countries='all', continents='all', xlog='linear', ylog='linear'):
    # https://plotly.com/python/hover-text-and-formatting/#advanced-hover-template
    # https://community.plotly.com/t/hover-data-on-go-scatter-and-or-shared-legends-with-plotly-express/34239
    fig_case_vac = go.Figure()
    
    if not countries == 'all':
        mat_countries = df['country_region'].isin(countries)
        df = df[mat_countries]

    if not continents == 'all':
        mat_continents = df['continent'].isin(continents)
        df = df[mat_continents]

    if y_col == 'past_week_daily_cases_per_100k':
        past_week_string = 'Past week daily average case per 100k'
        # y_decimal = 2
    else:
        past_week_string = 'Past week daily average'
        # y_decimal = 0

    if x_col == 'percent_adjusted_people_vaccinated':
        adj_vac_string = 'Percent adjusted people vaccinated'
        percent_symbol = '%'
    else:
        adj_vac_string = 'Number of adjusted people vaccinated'
        percent_symbol = ''

    for continent_name, continent in df.groupby('continent'):
        if continent_name == 'Africa':
            color = 'black'
        elif continent_name == 'Asia':
            color = 'yellow'
        elif continent_name == 'Europe':
            color = 'blue'
        elif continent_name == 'Oceania':
            color = 'green'
        elif continent_name == 'North America':
            color = 'red'
        elif continent_name == 'South America':
            color = 'cyan'

        fig_case_vac.add_traces(go.Scatter(x=continent[x_col],
                                        y=continent[y_col],
                                        mode='markers',
                                        hoveron='points',
                                        marker={'color': color},
                                        text=continent['country_region'],
                                        customdata=continent['date_string'],
                                        hovertemplate= 
                                        "Country/region: %{text}<br>" +
                                        "Latest status update date: %{customdata}<br>" +
                                        past_week_string + ": %{y: .2f}<br>" +
                                        adj_vac_string + ": %{x: .2f}" + percent_symbol + "<br>" +
                                        "<extra></extra>",
                                        name=continent_name)
                            )

    # Change marker size
    fig_case_vac.update_traces(marker=dict(size=12,
                                opacity=0.7,
                                line=dict(width=2,
                                            color='DarkSlateGrey')),
                    selector=dict(mode='markers'))
        
    # Turn background to transparent
    fig_case_vac.update_layout({
        'plot_bgcolor': 'rgba(0, 0, 0, 0)',
        })
    title_string = "Latest status plot: "
    if y_col == 'past_week_daily_cases':
        title_string = title_string + 'Past week daily cases'
    elif y_col == 'past_week_daily_cases_per_100k':
        title_string = title_string + 'Past week daily cases per 100K'

    if x_col == 'percent_adjusted_people_vaccinated':
        title_string = title_string + ' and vaccination rate (lastest data only)'
    else:
        title_string = title_string + ' and number of vaccination (lastest data only)'

    fig_case_vac.update_layout(
        title={
            'text': title_string
        })

    # Turn on x-axis and y-axis lines
    fig_case_vac.update_xaxes(showline=True, linewidth=1, linecolor='black', ticks="inside", type=xlog)
    # Title
    if x_col == 'percent_adjusted_people_vaccinated':
        x_label = 'Adjusted people vaccinated (%'
    else:
        x_label = 'Adjusted people vaccinated (Number of people'

    if xlog == 'log':
        x_label = x_label + '; log scale)'
    else:
        x_label = x_label + ')'

    fig_case_vac.update_xaxes(title_text=x_label)
    # Range
    # fig.update_xaxes(range=[df_curr_case_vac[x_col].min() - 2, df_curr_case_vac[x_col].max() + 2])


    fig_case_vac.update_yaxes(showline=True, linewidth=1, linecolor='black', ticks="inside", type=ylog)
    if y_col == 'past_week_daily_cases_per_100k':
        y_label = 'Past week daily average cases (per 100K'
    else:
        y_label = 'Past week daily average cases (number of people'
    
    if ylog == 'log':
        y_label = y_label + '; log scale)'
    else:
        y_label = y_label + ')'

    
    fig_case_vac.update_yaxes(title_text=y_label)
        
    # fig_case_vac.show()
    return fig_case_vac


def plot_case_vac_ts(df, country, x_col='percent_adjusted_people_vaccinated', y_col='past_week_daily_cases', xlog='linear', ylog='linear'):
    # https://plotly.com/python/hover-text-and-formatting/#advanced-hover-template
    # https://community.plotly.com/t/hover-data-on-go-scatter-and-or-shared-legends-with-plotly-express/34239
    fig_case_vac_ts = go.Figure()

    # list_country = ['United Kingdom', 'Israel', 'US', 'Australia', 'China']
    # list_country = ['United Kingdom']
    if isinstance(country, list):
        list_country = country
    else:
        list_country = [country]


    if y_col == 'past_week_daily_cases_per_100k':
        past_week_string = 'Past week daily average case per 100k'
        # y_decimal = 2
    else:
        past_week_string = 'Past week daily average'
        # y_decimal = 0

    if x_col == 'percent_adjusted_people_vaccinated':
        adj_vac_string = 'Percent adjusted people vaccinated'
        percent_symbol = '%'
    else:
        adj_vac_string = 'Number of adjusted people vaccinated'
        percent_symbol = ''

    for country in list_country:
        mat_country = df['country_region'] == country
        df_ts = df[mat_country]
        fig_case_vac_ts.add_traces(go.Scatter(x=df_ts[x_col],
                                        y=df_ts[y_col],
                                        mode='lines+markers',
                                        hoveron='points',
                                        text=df_ts['date_string'],
                                        customdata=df_ts['country_region'],
                                        hovertemplate= 
                                        "Country/region: %{customdata}<br>" + 
                                        "Date: %{text}<br>" +
                                        past_week_string + ": %{y: .2f}<br>" +
                                        adj_vac_string + ": %{x: .2f}" + percent_symbol + "<br>" +
                                        "<extra></extra>",
                                        name=country)
                            )

    # # Change marker size
    # fig_case_vac_one_country.update_traces(marker=dict(size=12,
    #                               opacity=0.8,
    #                               line=dict(width=2,
    #                                         color='DarkSlateGrey')),
    #                   selector=dict(mode='markers'))
        
    # # Turn background to transparent
    fig_case_vac_ts.update_layout({
            'plot_bgcolor': 'rgba(0, 0, 0, 0)'
        })

    title_string = "All available vaccination plot: "
    
    if y_col == 'past_week_daily_cases':
        title_string = title_string + 'Past week daily cases'
    elif y_col == 'past_week_daily_cases_per_100k':
        title_string = title_string + 'Past week daily cases per 100K population'

    if x_col == 'percent_adjusted_people_vaccinated':
        title_string = title_string + ' and vaccination rate (all available data)'
    else:
        title_string = title_string + ' and number of vaccination (all available data)'
    
    fig_case_vac_ts.update_layout(
        title={
            'text': title_string
        })

    # Turn on x-axis and y-axis lines
    fig_case_vac_ts.update_xaxes(showline=True, linewidth=1, linecolor='black', ticks="inside", type=xlog)
    # Title
    if x_col == 'percent_adjusted_people_vaccinated':
        x_label = 'Adjusted people vaccinated (%'
    else:
        x_label = 'Adjusted people vaccinated (Number of people'

    if xlog == 'log':
        x_label = x_label + '; log scale)'
    else:
        x_label = x_label + ')'

    fig_case_vac_ts.update_xaxes(title_text=x_label)
    # # Range
    # # fig.update_xaxes(range=[df_curr_case_vac[x_col].min() - 2, df_curr_case_vac[x_col].max() + 2])


    fig_case_vac_ts.update_yaxes(showline=True, linewidth=1, linecolor='black', ticks="inside", type=ylog)
    if y_col == 'past_week_daily_cases_per_100k':
        y_label = 'Past week daily average cases (per 100K'
    else:
        y_label = 'Past week daily average cases (number of people'
    
    if ylog == 'log':
        y_label = y_label + '; log scale)'
    else:
        y_label = y_label + ')'
        
    fig_case_vac_ts.update_yaxes(title_text=y_label)
        
    # fig_case_vac_ts.show()
    
    return fig_case_vac_ts 
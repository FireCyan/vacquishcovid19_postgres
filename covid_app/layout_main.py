# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 14:30:07 2021

@author: john.yang
"""
###############################################################################
# Initial imports

import dash
import dash_core_components as dcc
import dash_html_components as html
# import dash_table
import plotly
import plotly.express as px
import plotly.tools as tls
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import urllib
import json

import os, sys, re 
from pathlib import Path 
import pandas as pd
import numpy as np

# from jupyter_dash import JupyterDash
import base64

import query_processing as qp
###############################################################################


df_case_loc_vac = qp.combine_case_vac_lookup()

# TODO: fill in the list of countries/regions available
# list_of_countries = ['Australia', 'US', 'China']
# list_of_continents = ['Africa', 'Asia', 'Europe', 'North America', 'Oceania', 'South America']
list_of_countries = list(df_case_loc_vac.dropna(subset=['total_vaccinations'])['country_region'].unique())
list_of_continents = list(df_case_loc_vac['continent'].dropna().unique())

# default countries/regions to show
# selected_countries = list(dfl.sort_values('Confirmed', ascending=False)['country'].iloc[0:5])
selected_countries = ['Australia', 'Brazil', 'Canada', 'China', 'Egypt', 'France', 'Ghana', 'Germany', 'India', 'Israel', 'Japan', 'Korea, South', 'Mexico', 'New Zealand', 'Peru', 'South Africa', 'United Kingdom', 'US']
selected_continents = list_of_continents

# if 'AWS' in os.environ:
#     logo_image = 's3_path/image.png'
# else:
#     logo_image = '.\static\image.png'
# encoded_image = base64.b64encode(open(logo_image, 'rb').read())

layout = html.Div(
    children=[
        ##################
        # Logo and title
        ##################
        # dbc.Row([
        #     dbc.Col(
        #         html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()),
        #                 style=dict(
        #                     width='120%'
        #                     ),
        #                 ),
        #         width=1
        #         ),
        #     dbc.Col(
        #         html.H2('Cyan Covid-19 Analytics Dashboard'),
        #         width=6
        #         ),
        #     ],
        #     style=dict(
        #         # verticalAlign='middle',
        #         marginLeft='0%'
        #     )
        # ),
        ##### Nav bar #####
        # dbc.Row([
        #     dbc.NavbarSimple(
        #         children=[
        #             dbc.NavItem(dbc.NavLink("Covid-19 cases & vaccinations", href="#")),

        #         ])
        # ])
    
    ##########################################
    ##########################################
    # Main content: Parameter panel and plots
    ##########################################
    ##########################################
        dbc.Row([
            ####################
            # Parameter panel
            ####################
            dbc.Col(
                html.Div(
                    children=[
                        html.Div(
                                children=[
                                    html.H3("COVID-19 CASES & VACCINATION"),
                                    html.Span(
                                        """Select a range of countries to compare past week daily average cases and vaccination number/rates"""
                                    ),                    
                                    ]
                                ),
                        
                        html.H4("Latest status plot"),
                        
                        html.Div(
                            children=[
                                html.Span('Use continent or country/region for latest status plot'),
                                dcc.Dropdown(
                                    id="plot_source_latest",
                                    options=[
                                        {"label": i, "value": i}
                                        for i in ['Continent', 'Country/region']
                                    ],
                                    value='Country/region',
                                    multi=False,
                                    searchable=False,
                                    clearable=False
                                )                              
                            ],
                            style=dict(
                                # verticalAlign='middle',
                                marginBottom='3%',
                            )
                        ),        
                        
                        html.Div(
                            children=[
                                html.Span('Select continents for latest status plot'),
                                dcc.Dropdown(
                                    id="continent_dropdown_latest",
                                    options=[
                                        {"label": i, "value": i}
                                        for i in list_of_continents
                                    ],
                                    value=selected_continents,
                                    multi=True,
                                )                              
                            ],
                            style=dict(
                                # verticalAlign='middle',
                                marginBottom='3%',
                            )
                        ),
                        
                        html.Div(
                            children=[
                                html.Span('Select countries/regions for latest status plot'),
                                dcc.Dropdown(
                                    id="country_dropdown_latest",
                                    options=[
                                        {"label": i, "value": i}
                                        for i in list_of_countries
                                    ],
                                    value=selected_countries,
                                    multi=True,
                                    # style={ "overflow-y":"scroll", "height": "100px"},
                                )                              
                            ],
                            style=dict(
                                # verticalAlign='middle',
                                marginBottom='3%',
                            )
                        ),

                        html.Div([
                            html.Span('Click to fill in countries/regions from continents selected'),
                            html.Button('Fill', id='fill_cr', n_clicks=0)]),

                        # Horizontal line to fill break between 2 plotting panels
                        html.Hr(),

                        #################################
                        # All available data plotting
                        #################################
                        
                        html.H4("All available vaccination plot"),

                        html.Div(
                            children=[
                                html.Span('Select countries/regions for all available vaccination plot'),
                                dcc.Dropdown(
                                    id="country_dropdown_all",
                                    options=[
                                        {"label": i, "value": i}
                                        for i in list_of_countries
                                    ],
                                    value=selected_countries,
                                    multi=True,
                                )                              
                            ],
                            style=dict(
                                # verticalAlign='middle',
                                marginBottom='3%',
                            )
                        ),

                        # Horizontal line to fill break between 2 plotting panels
                        # html.Hr(),

                        
                    ],
                ),
                width=3,
                style=dict(
                    # verticalAlign='middle',
                    marginTop='1%',
                    marginLeft='1%',
                )
            ),
            ##########
            # Plots 
            ##########
            # The whole plot section takes 9 width.
            # Have 2 graphs with 8 width and half of the row
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dcc.Graph(
                        id='case_vac_latest',
                        config={
                            'displayModeBar': False
                        })
                    ],
                    width=12,
                    ),
                    # dbc.Col([
                    #     dcc.Graph(
                    #     id='per_100k_case_vac_latest',
                    #     config={
                    #         'displayModeBar': False
                    #     })
                    # ],
                    # width=6,
                    # ),
                    ]
                    ),

                # html.Hr(style=dict(
                #     color='dark'
                # )),

                dbc.Row([
                    dbc.Col([
                        dcc.Graph(
                        id='case_vac_time_series',
                        config={
                            'displayModeBar': False
                        })
                    ],
                    width=12,
                    ),
                    # dbc.Col([
                    #     dcc.Graph(
                    #     id='per_100k_case_vac_time_series',
                    #     config={
                    #         'displayModeBar': False
                    #     })
                    # ],
                    # width=6,
                    # ),
                    ]
                    ),
                ],
                width=7,
            ),
            
            ################################################
            # Dropdowns to change value type and axis-scale
            ################################################

            dbc.Col(
                html.Div(
                    children=[
                        # Select Vaccination value type 
                        html.Span('Vaccination value type'),
                        dcc.Dropdown(
                            id="vac_value_type",
                            options=[
                                {"label": i, "value": i}
                                for i in ['Percentage', 'Absolute number']
                            ],
                            placeholder='Select vaccination value type',
                            value='Percentage',
                            multi=False,
                            searchable=False,
                            clearable=False
                            ),
                        

                    
                        # Select daily case value type
                        html.Span('Past week daily case value type'),
                        dcc.Dropdown(
                            id="case_value_type",
                            options=[
                                {"label": i, "value": i}
                                for i in ['Per 100K', 'Absolute number']
                            ],
                            placeholder='Select case value type',
                            value='Per 100K',
                            multi=False,
                            searchable=False,
                            clearable=False
                        ),

                        # Horizontal line 
                        html.Hr(),

                        # Select x-axis scale
                        html.Span('x-axis scale'),
                        dcc.Dropdown(
                            id="x_axis_log",
                            options=[
                                {"label": i, "value": i}
                                for i in ['Normal scale', 'Log scale']
                            ],
                            placeholder='Select x-axis scale',
                            value='Normal scale',
                            multi=False,                                    
                            searchable=False,
                            clearable=False
                            ),
                        # Select y-axis scale
                        html.Span('y-axis scale'),
                        dcc.Dropdown(
                            id="y_axis_log",
                            options=[
                                {"label": i, "value": i}
                                for i in ['Normal scale', 'Log scale']
                            ],
                            placeholder='Select y-axis scale',
                            value='Normal scale',
                            multi=False,                                    
                            searchable=False,
                            clearable=False
                        ),
                        # Horizontal line 
                        html.Hr(),

                        ###############################################
                        # Plot information 
                        # Plot description and instruction + equations
                        ###############################################
                        html.Div([
                            html.H6("Plot info"),
                            html.Div([
                                html.A('Plot description',href='https://vacquishcovid19.s3-ap-southeast-2.amazonaws.com/plot_desc_instruction.pdf', target='_blank'),
                            ]),
                            html.Div([
                                html.A('Equations',href='https://vacquishcovid19.s3-ap-southeast-2.amazonaws.com/equations.pdf', target='_blank'),
                            ])
                        ]),

                        html.Hr(),

                        
                        html.Div([
                            # #################
                            # # Data source URL
                            # ##################
                            html.H6("Data source"),
                            # # For markdown next line problem
                            # # https://community.plotly.com/t/transform-text-string-to-properly-display-new-line-and-spaces-in-dcc-markdown/28937
                            html.Div([
                                html.Span('Daily cases: '),
                                html.A('JHU CSSE COVID-19 Data',href='https://github.com/CSSEGISandData/COVID-19', target='_blank',
                                # style=dict(
                                #     paddingTop='50px',
                                # )
                                ),
                            ]),

                            html.Div([
                                html.Span('Vaccination number/rates: '),
                                html.A('Our World in Data',href='https://github.com/owid/covid-19-data/tree/master/public/data', target='_blank'),
                            ]),

                            
                            
                            # dcc.Markdown(
                            #     children=[
                            #         "Daily cases: [Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)\n",
                            #         "Vaccination number/rates: [World in Data](https://github.com/owid/covid-19-data/tree/master/public/data)\n",
                            #         "CSS styling: [Dark mode CSS](https://github.com/sreejithmunthikodu/TrackingCovid19/tree/master/assets)"
                            #         ],
                            #         ),
                            # ##################
                            # # Acknowledgement
                            # ##################
                            html.H6("ACK"),
                            
                            html.Div([
                                html.Span('CSS styling: '),
                                html.A('Dark mode CSS',href='https://github.com/sreejithmunthikodu/TrackingCovid19/tree/master/assets', target='_blank'),
                            ]),
                            html.Div([
                                html.Span('Plot idea: '),
                                html.A('Covid Trends',href='https://aatishb.com/covidtrends/', target='_blank'),
                            ]),
                            html.Div([
                                html.Span('Thanks to my friend MJ who showed me the video for the plot idea above'),
                            ]),
                            # dcc.Markdown(
                            #     children=[
                            #         "Plot idea: [Covid Trends](https://aatishb.com/covidtrends/)\n",
                            #         "Thanks to my friend MJ who showed me the video for the plot above"
                            #         ],
                            #         ),
                        ])
                    ]
                ),
                style=dict(
                    # verticalAlign='middle',
                    marginTop='3%',
                    marginRight='1%',
                )
            )
        ])

])

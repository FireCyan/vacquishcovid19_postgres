# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 22:47:07 2021

@author: john.yang

Functions that query and process df for plotting
"""

###############################################################################
# Initial imports
import os, sys, re 
from pathlib import Path

# current_wd = Path(r'C:\John_folder\github_projects\vacquishcovid19_postgres')

# https://stackoverflow.com/questions/595305/how-do-i-get-the-path-of-the-python-script-i-am-running-in
current_file_path = Path(os.path.realpath(__file__))

# https://stackoverflow.com/questions/2860153/how-do-i-get-the-parent-directory-in-python
current_wd = current_file_path.parent.parent.absolute()

# print(current_wd)

modules=[current_wd]

for module in modules: 
    for fld in module.glob('**'): 
        if re.search('__pycache__', str(fld)) is None and re.search('\.git', str(fld)) is None and str(fld) not in sys.path: 
            sys.path.append(str(fld))


import etl
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import configparser
import os
import aws_util

###############################################################################

pd.options.mode.chained_assignment = None  # default='warn'

# df_test = pd.DataFrame([datetime.now(), datetime.now()+timedelta(days=1)], columns=['date'])

config = configparser.ConfigParser()
config.read('dl.cfg')

# conn = psycopg2.connect("host=127.0.0.1 dbname=covid19_db user=postgres password=localtest")
# cur = conn.cursor()

dbname = 'covid19_db'
conn, cur = aws_util.conn_db(dbname)

def combine_case_vac_lookup():
    ##### Case df #####
    case_sql = """
        SELECT date_string, Country_Region, sum(Confirmed) as country_confirmed, sum(deaths) as country_death
        FROM daily_case
        GROUP BY date_String, Country_Region
    """

    df_case = pd.read_sql(case_sql, conn)
    mat_country_region_null = pd.isnull(df_case['country_region'])
    df_case = df_case[~mat_country_region_null]

    ##### Vac df #####
    vac_sql = """
        SELECT *
        FROM vac
    """

    df_vac = pd.read_sql(vac_sql, conn)

    ##### lookup df #####
    cl_sql = """
        SELECT *
        FROM country_loc
    """

    df_cl = pd.read_sql(cl_sql, conn)

    ##### Combining #####
    mat_prov_null = pd.isnull(df_cl['province_state']) | (df_cl['province_state'] == '') # province_state = Null so that we are getting country/region, instead of province/state granularity
    df_cl_for_comb = df_cl[mat_prov_null][['country_region', 'location', 'population', 'continent']]
    df_case_loc = df_case.merge(df_cl_for_comb, on='country_region', how='left')

    df_case_loc_vac = df_case_loc.merge(df_vac, on=['location', 'date_string'], how='left')

    return df_case_loc_vac

def get_adjusted_people_vaccinated(df):
    df_case_vac = df.copy()


    df_case_vac['date'] = df_case_vac['date_string'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df_case_vac = df_case_vac.sort_values('date')


    #####################################################
    # Past week daily average confirmed cases and death
    #####################################################

    # Get daily cases by using diff function
    df_case_vac[['daily_country_confirmed', 'daily_country_death']] = df_case_vac.groupby('country_region')[['country_confirmed', 'country_death']].diff()
    # df_case_vac['daily_country_confirmed'] = df_case_vac['daily_country_confirmed'].fillna(0)


    ##### Handle negative values and get rolling average for confirmed cases #####
    # To deal with the issue where France case was retrospectively updated on 2021-05-26
    mat_case_neg = (df_case_vac['daily_country_confirmed'] < 0)
    df_case_vac.loc[mat_case_neg, 'daily_country_confirmed'] = np.nan

    # Get past week daily average cases
    # df_case_vac['past_week_daily_cases'] = (df_case_vac.groupby('country_region')['daily_country_confirmed'].
    # rolling(window=7, min_periods=7).mean().reset_index(0,drop=True))
    # After making negative values NAN, change the rolling here min_periods to 1 so that not taking those negative values into calculating 7 days average
    df_case_vac['past_week_daily_cases'] = (df_case_vac.groupby('country_region')['daily_country_confirmed'].
    rolling(window=7, min_periods=1).mean().reset_index(0,drop=True))


    ##### Handle negative values and get rolling average for death #####
    mat_death_neg = (df_case_vac['daily_country_death'] < 0)
    df_case_vac.loc[mat_death_neg, 'daily_country_death'] = np.nan

    df_case_vac['past_week_daily_death'] = (df_case_vac.groupby('country_region')['daily_country_death'].
    rolling(window=7, min_periods=1).mean().reset_index(0,drop=True))


    ###########################################################################################################################################
    # Vaccination part
    # Calculated adjusted_people_vaccinated and determine which countries/regions have total_vaccinations and people_vaccinated
    # Explanation:
    # Most vaccination needs 2 dosages, so total_vaccinations != people_vaccinated != people_fully_vaccinated
    # Therefore, to calculate vaccination number, I call it adjusted_people_vaccinated
    # Note that:
    # Some countries/regions only have no total_vaccinations and so cannot be used for vaccination analysis
    # Some countries/regions only have total_vaccinations but no people_vaccinated
    # Most countries/regions have both total_vaccinations and people_vaccinated
    # 
    # The equations for those with at least total_vaccinations:
    # For countries/regions where people_vaccinated values are available:
    # adjusted_people_vaccinated = 1*people_fully_vaccinated + 0.5*(people_vaccinated - people_fully_vaccinated)
    #
    # For countries/regions where only total_vaccinations values (but not people_vaccinated) are available
    # adjusted_people_vaccinated = 0.5*(total_vaccinations) # given that all vaccination brand used by these countries/regions require 2 doses
    ###########################################################################################################################################

    # TODO: fix the problem of some countries changing to having only total_vaccinations later (eg, Australia)

    # df_temp = df_case_vac.dropna(subset=['people_vaccinated'])
    # list_countries_before_drop = list(df['country_region'].unique())
    # list_countries_after_drop_null_ppl_vaccinated = list(df_temp['country_region'].unique())
    df_case_vac_drop_null_total = df.dropna(subset=['total_vaccinations'])
    list_countries_after_drop_null_total_vac = list(df_case_vac_drop_null_total['country_region'].unique())

    df_case_vac_latest_with_ppl_vac = df_case_vac_drop_null_total.groupby('country_region').tail(1).dropna(subset=['people_vaccinated'])

    list_countries_with_ppl_vac = list(df_case_vac_latest_with_ppl_vac['country_region'].unique())
    
    # countries that do not have values for people_vaccinated
    # list_country_no_ppl_vac = [x for x in list_countries_before_drop if x not in list_countries_after_drop_null_ppl_vaccinated]
    # countires that do not have values for total_vaccinations
    # list_country_no_total_vac = [x for x in list_countries_before_drop if x not in list_countries_after_drop_total_vac]
    # print(list_country_dropped)
    # print(list_country_no_total_vac)

    # countries that have total_vaccinations but no people_vaccinated
    list_country_with_total_vac_no_ppl_vac = [x for x in list_countries_after_drop_null_total_vac if x not in list_countries_with_ppl_vac]
    # print(list_country_with_total_vac_no_ppl_vac)
    # print(len(list_countries_after_drop_null_ppl_vaccinated))

    # Calculate percentage of adjusted_people_vaccinated
    # For countries/regions where people_vaccinated values are available:
    # adjusted_people_vaccinated = 1*people_fully_vaccinated + 0.5*(people_vaccinated - people_fully_vaccinated)

    df_case_vac['adjusted_people_vaccinated'] = np.nan

    mat_ppl_vac = df_case_vac['country_region'].isin(list_countries_with_ppl_vac)

    df_case_vac.loc[mat_ppl_vac, 'adjusted_people_vaccinated'] = \
    (df_case_vac.loc[mat_ppl_vac, 'people_fully_vaccinated'].fillna(0) + 
    0.5*(df_case_vac.loc[mat_ppl_vac, 'people_vaccinated'].fillna(0) - 
        df_case_vac.loc[mat_ppl_vac, 'people_fully_vaccinated'].fillna(0)))

    # For countries/regions where only total_vaccinations values (but not people_vaccinated) are available
    # adjusted_people_vaccinated = 0.5*(total_vaccinations) # given that all vaccination brand used by these countries/regions require 2 doses
    mat_total_vac = df_case_vac['country_region'].isin(list_country_with_total_vac_no_ppl_vac)
    df_case_vac.loc[mat_total_vac, 'adjusted_people_vaccinated'] = 0.5*df_case_vac.loc[mat_total_vac, 'total_vaccinations'].fillna(0)


    ##### Combine those with total_vaccinations only and those with both total_vaccinations and people_vaccinated together
    df_case_vac_ppl_vac = df_case_vac[mat_ppl_vac].dropna(subset=['people_vaccinated'])
    df_case_vac_total_vac = df_case_vac[mat_total_vac].dropna(subset=['total_vaccinations'])
    df_case_vac = pd.concat([df_case_vac_ppl_vac, df_case_vac_total_vac])


    ##### Calculate daily cases per 100k and percentage of adjusted people vaccinated #####
    # df_curr_case_vac
    df_case_vac['past_week_daily_cases_per_100k'] = (df_case_vac['past_week_daily_cases']/df_case_vac['population']*100000)
    

    df_case_vac['percent_adjusted_people_vaccinated'] = (df_case_vac['adjusted_people_vaccinated']/df_case_vac['population']*100)

    # Get the max date row with groupby 
    df_curr_case_vac = (df_case_vac[df_case_vac.groupby('country_region')['date'].transform('max') == df_case_vac['date']])


    return df_case_vac, df_curr_case_vac


# -*- coding: utf-8 -*-
"""
Created on Thu May  6 22:08:51 2021

@author: john
"""

import os, sys, re
import pandas as pd
from pathlib import Path

current_wd = Path(os.getcwd())

modules=[current_wd]

for module in modules: 
    for fld in module.glob('**'): 
        if re.search('__pycache__', str(fld)) is None and re.search('\.git', str(fld)) is None and str(fld) not in sys.path: 
            sys.path.append(str(fld))

import aws_util



##############################
# Download files from sources
##############################
import download_file

print('--------- Start downloading updated data -------------')

print('Download daily case data from JHU CSSE...')
download_file.dl_case_files()
print('Finish downloading updated daily case data from JHU CSSE\n')


print('Download vaccination data from World in Data...')
download_file.dl_vac_files()
print('Finish downloading updated vaccination data from Wold in Data\n')


print('Download lookup tables from JHU CSSE and World in Data...')
download_file.dl_lookup_files()
print('Finish downloading updated lookup tables from JHU CSSE and World in Data\n')

print('--------- Finish downloading updated data -------------')


#########################
# Create DB and tables
#########################
import create_table

# Only run create_table.main() for the first time building the DB
# create_table.main()

###############
# Perform ETL
###############

import etl


#### daily case processing #####
query, tuples = etl.process_case_data(process_all=False)


##### vaccination processing #####
etl.process_vaccine_data(process_all=True)


##### country location processing #####
etl.process_country_location()


##### Data check #####
# Need to have the relevant files in local folder in order to work
# etl.check_case_time_table()
# etl.check_vac_table()
# etl.check_country_loc_table()
# etl.check_csv_file_record()


###################################
# Data analysis and visualisation
###################################
import query_processing as qp
from plotly.offline import plot

df_case_loc_vac = qp.combine_case_vac_lookup()
df_case_vac, df_curr_case_vac = qp.get_adjusted_people_vaccinated(df_case_loc_vac)

import covid_plot as cp

fig = cp.plot_latest_case_vac(df_curr_case_vac, y_col='past_week_daily_cases_per_100k')

plot(fig)

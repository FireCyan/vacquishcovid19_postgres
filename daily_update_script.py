# -*- coding: utf-8 -*-

import download_file
import etl

##########################
# Download updated data
##########################
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



###################
# Update database
###################
print('--------- Start updating database tables -------------')

print('Start updating daily_case table...')
etl.process_case_data(process_all=False)
print('Finish updating daily_case table')


print('Start updating vac table...')
etl.process_vaccine_data(process_all=True)
print('Finish updating vac table')


print('Start updating country_loc table...')
etl.process_country_location()
print('Finish updating country_loc table')


print('--------- Finish updating database tables -------------')
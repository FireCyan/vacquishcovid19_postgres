import requests
import io
from github import Github
# For github module, see
# # https://pygithub.readthedocs.io/en/latest/introduction.html

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import configparser
import os


config = configparser.ConfigParser()
config.read('dl.cfg')

import aws_util


def dl_case_files():

    # Define what date csv files should be downloaded into old format folder
    old_format_dates = [datetime.strptime('2020-01-22', '%Y-%m-%d'), datetime.strptime('2020-03-21', '%Y-%m-%d')]

    #############################################################
    # Get a list of source files from JHU CSSE Github repository
    #############################################################
    from github import Github

    # https://github.com/PyGithub/PyGithub/issues/1741
    # If working with a public Github without authentication, just do Github()
    g = Github()

    # Get the repository
    repo = g.get_repo("CSSEGISandData/COVID-19")

    # Get the content from a specific folder of the repository
    # https://stackoverflow.com/questions/53527783/pygithub-how-to-get-contents-of-the-subfolder-in-the-repo
    contents = repo.get_contents("csse_covid_19_data/csse_covid_19_daily_reports")

    list_source_case_files = []
    dict_source_file_url = {}

    # print(contents)
    while len(contents) > 0:
        file_content = contents.pop(0)
        if file_content.type == 'dir':
            contents.extend(repo.get_contents(file_content.path))
        else:
            filename = file_content.path.split('/')[-1]
            if '.csv' in filename:
    #             print(filename)
                list_source_case_files.append(filename)
                dict_source_file_url[filename] = file_content.download_url
    #             print(file_content.download_url)
    #             print(file_content.last_modified)
    #         print(file_content.path)

    # print(list_source_case_files)

    ###########################################
    # Get the files available in local storage
    ###########################################

    case_data_path = config['STORAGE']['CASE_DATA']
    case_old_data_path = config['STORAGE']['CASE_DATA_OLD_FORMAT']
    # file_all = os.listdir(case_data_path) # all files in case_data_path
    # file_all_old = os.listdir(case_old_data_path) # all files in case_old_data_path
    file_all = aws_util.list_files(case_data_path)
    file_all_old = aws_util.list_files(case_old_data_path)

    list_local_case_files = file_all + file_all_old
    # print(list_local_case_files)

    ###################################################
    # Get missing files from local against source list
    ###################################################
    # Download files
    # https://github.com/PyGithub/PyGithub/issues/1343
    # Read to Pandas
    # https://stackoverflow.com/questions/32400867/pandas-read-csv-from-url
    # Write to file using .to_csv

    list_miss_files = [x for x in list_source_case_files if x not in list_local_case_files]

    for f in list_miss_files:
        f_date = datetime.strptime(f.split('.')[0], '%m-%d-%Y')
        print(f)
        s = requests.get(dict_source_file_url[f]).content
        df_csv = pd.read_csv(io.StringIO(s.decode('utf-8')))
    #     print(dict_source_file_url[f])
        if (f_date >= old_format_dates[0]) & (f_date <= old_format_dates[1]):
            # Save to old format folder
            # df_csv.to_csv(os.path.join(case_old_data_path, f), index=False)
            
            uploaded = aws_util.upload_to_aws(df=df_csv, fname=os.path.join(case_old_data_path, f))

        else:
            # Save to regular folder
            # df_csv.to_csv(os.path.join(case_data_path, f), index=False)

            uploaded = aws_util.upload_to_aws(df=df_csv, fname=os.path.join(case_data_path, f))

def dl_vac_files():
    ##########################################################################
    # Download vaccination source files from World in Data Github repository
    ##########################################################################

    vaccine_data_path = config['STORAGE']['VAC_DATA']

    # https://github.com/PyGithub/PyGithub/issues/1741
    # If working with a public Github without authentication, just do Github()
    g = Github()

    # Get the repository
    repo = g.get_repo("owid/covid-19-data")

    # Get the content from a specific folder of the repository
    # https://stackoverflow.com/questions/53527783/pygithub-how-to-get-contents-of-the-subfolder-in-the-repo
    contents = repo.get_contents("public/data/vaccinations/country_data")

    # For vaccination date, would need to just download everything because the files are organised as country.csv
    # And so daily update is update to the country.csv file rather than adding new files.
    while len(contents) > 0:
        file_content = contents.pop(0)
        if file_content.type == 'dir':
            contents.extend(repo.get_contents(file_content.path))
        else:
            filename = file_content.path.split('/')[-1]
            if '.csv' in filename:
                print(filename)            
                s = requests.get(file_content.download_url).content
                df_csv = pd.read_csv(io.StringIO(s.decode('utf-8')))
                # df_csv.to_csv(os.path.join(vaccine_data_path, filename), index=False)

                uploaded = aws_util.upload_to_aws(df=df_csv, fname=os.path.join(vaccine_data_path, filename))


def dl_lookup_files():
    country_path = config['STORAGE']['CASE_LOOKUP']
    location_path = config['STORAGE']['VAC_LOOKUP']

    # https://github.com/PyGithub/PyGithub/issues/1741
    # If working with a public Github without authentication, just do Github()
    g = Github()

    # Get the repository
    repo = g.get_repo("CSSEGISandData/COVID-19")

    # Get the content from a specific folder of the repository
    # https://stackoverflow.com/questions/53527783/pygithub-how-to-get-contents-of-the-subfolder-in-the-repo
    contents = repo.get_contents("csse_covid_19_data")

    # print(contents)
    while len(contents) > 0:
        file_content = contents.pop(0)
        filename = file_content.path.split('/')[-1]
        if filename == 'UID_ISO_FIPS_LookUp_Table.csv':
            print(filename)
            s = requests.get(file_content.download_url).content
            df_csv = pd.read_csv(io.StringIO(s.decode('utf-8')))
            # df_csv.to_csv(country_path, index=False)
            
            uploaded = aws_util.upload_to_aws(df=df_csv, fname=country_path)
            
    ##### vaccination lookup part #####
    g = Github()

    # Get the repository
    repo = g.get_repo("owid/covid-19-data")

    # Get the content from a specific folder of the repository
    # https://stackoverflow.com/questions/53527783/pygithub-how-to-get-contents-of-the-subfolder-in-the-repo
    contents = repo.get_contents("public/data/jhu")

    # print(contents)
    while len(contents) > 0:
        file_content = contents.pop(0)
        filename = file_content.path.split('/')[-1]
        if filename == 'locations.csv':
            print(filename)
            s = requests.get(file_content.download_url).content
            df_csv = pd.read_csv(io.StringIO(s.decode('utf-8')))
            # df_csv.to_csv(location_path, index=False)

            uploaded = aws_util.upload_to_aws(df=df_csv, fname=location_path)
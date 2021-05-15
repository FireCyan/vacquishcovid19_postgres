'''
Author: John Yang

'''

import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
import configparser
from datetime import datetime, date, timedelta

import aws_util

config = configparser.ConfigParser()
config.read('dl.cfg')


today = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

dbname = 'covid19_db'

#########################
#########################
#
# Data Processing part
#
#########################
#########################

def preprocess_case_data(list_old='all'):
    '''
    The column names for case data before 2020-03-21 (inclusive) are slightly different from those after.
    To fix this problem, run this function to update the column name and overwrite the csv files
    
    Args:
        NA
        
    Update column names of csv files in old format
        
    Returns:
        NA
    '''
    
    print("----- Start pre-processing old format case data -----")
#     case_data_path = './daily_case_data/old_format'
    case_old_data_path = config['STORAGE']['CASE_DATA_OLD_FORMAT']
    column_update_dict = {'Province/State': 'Province_State', 'Country/Region': 'Country_Region', 'Last Update': 'Last_Update', 'Latitude': 'Lat', 'Longitude': 'Long_'}

    if list_old == 'all':
        file_all_old = aws_util.list_files(case_old_data_path)
    else:
        file_all_old = list_old
    
    
    for filename in file_all_old:
        if filename.endswith(".csv"):
            # if using local#
            # df = pd.read_csv(os.path.join(case_old_data_path, filename))

            # if using AWS
            df = pd.read_aws_csv(os.path.join(case_old_data_path, filename))
            
            if 'Province/State' in df.columns.tolist():
                df = df.rename(columns=column_update_dict)
                # df.to_csv(os.path.join(case_old_data_path, filename), index=False)
                uploaded = aws_util.upload_to_aws(df=df, fname=os.path.join(case_old_data_path, filename))
#                 print(df.columns.tolist())
    
    print("----- Successfully Pre-processed old format case data and over-write the old one -----")
    print("---------------------------------------")


def process_case_data(process_all=False):
    """
    Process case data
        1. For loop through the case csv file
        2. Process csv files
        3. Insert the processed dataframe to database
        4. Insert the name of csv files processed into csv_record table (as a record)

    Notice that the columns are different before and after 2020-03-22.
    To address this problem, the data after 2020-03-22 would be loaded into df_new while those before would be loaded into df_old
    Then there is addition of columns with Null values for those missing columns in df_old (the old format data)
       
    Args:
        conn: psycopg2 connection
        cur: psycopg2 connection cursor
        process_all: whether to process all files (True) or just the files that are not recorded in the csv_record table (False)
    
    Upsert to tables:
        daily_case
        dim_time
        csv_record
    
    Returns:
        NA
    """
    col_to_include = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'date_string', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered', 'Active']

    case_data_path = config['STORAGE']['CASE_DATA']
    case_old_data_path = config['STORAGE']['CASE_DATA_OLD_FORMAT']

    csv_record_cols = ['file_name', 'data_type', 'last_update']
    
    process_name = "daily case"
    
    print("----- Start processing {} data -----".format(process_name))

    ##############################################
    # Determine what case csv files to process
    ##############################################
    ##### Get the list of case csv files that have been processed before #####
    
    # If using localhost
    # file_all = os.listdir(case_data_path) # all files in case_data_path
    # file_all_old = os.listdir(case_old_data_path) # all files in case_old_data_path

    conn, cur = aws_util.conn_db(dbname)

    # If using AWS
    file_all = aws_util.list_files(case_data_path)
    file_all_old = aws_util.list_files(case_old_data_path)

    if not process_all:
        print('Determining which {} csv files to process based on csv_record table...'.format(process_name))
        sql = csv_select % ('case')
        df_csv_record = pd.read_sql_query(sql, conn)
        file_processed_list = df_csv_record['file_name'].to_list() # files that have been recorded in db and processed before
        # print(file_processed_list)

        file_to_process = [x for x in file_all if not x in file_processed_list] # files that are not recorded in db (not processed before)
        file_old_to_process =  [x for x in file_all_old if not x in file_processed_list]
    else:
        file_to_process = file_all
        file_old_to_process = file_all_old

    conn.close() # Close for now because the processing would take a long time and connection might be lost by then.

    ########################################
    # Load + process case data (new format)
    ########################################
    print('Loading and processing {} data...'.format(process_name))
    df_all = pd.DataFrame() # df_all to hold all the processed records to be bulk inserted into db
    csv_list = [] # csv_list to hold tuples that contain name of the file that has been processed + source file type + process date
    df_time = pd.DataFrame()
    for filename in file_to_process:
        if filename.endswith(".csv"):
            # if using local
            # df_temp = pd.read_csv(os.path.join(case_data_path, filename)) # Temporary dataframe to hold loaded csv file
            print(filename)

            # if using AWS
            df_temp = pd.read_aws_csv(os.path.join(case_data_path, filename))
            
            

            csv_list.append(tuple([filename, 'case', today]))
            
            ##### case data processing #####
            df_temp['date_string'] = df_temp['Last_Update'].apply(to_date_string)
            df_temp['timestamp'] = df_temp['Last_Update'].apply(to_timestamp)
            # df_temp['Confirmed'] = df_temp['Confirmed'].fillna(0.0).astype(int)
            # df_temp['Confirmed'] = df_temp['Confirmed'].fillna(np.nan).replace([np.nan], [None])
            # df_temp['Deaths'] = df_temp['Deaths'].fillna(np.nan).replace([np.nan], [None])
            # df_temp['Recovered'] = df_temp['Recovered'].fillna(np.nan).replace([np.nan], [None])
            # df_temp['Active'] = df_temp['Active'].fillna(np.nan).replace([np.nan], [None])
            # for col in df_temp.columns.to_list():
            #     # standardise na into None so that in insertion they would be Null
            #     df_temp[col] = df_temp[col].fillna(np.nan).replace([np.nan], [None])

            df_all = pd.concat([df_all, df_temp])

    ##### time processing #####
    if not df_all.empty:
        t = df_all['timestamp']
        time_data = (t.dt.strftime('%Y-%m-%d %H:%M:%S'), t.dt.strftime('%Y-%m-%d'), t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
        time_col = ('Last_Update', 'date_string', 'hour', 'day', 'week', 'month', 'year', 'weekday')
        df_time = pd.DataFrame(dict(zip(time_col, time_data)))


        df_all = df_all[col_to_include]

    ########################################
    # Load + process case data (old format)
    ########################################
    # df_all = pd.DataFrame()
    # df_time = pd.DataFrame()
    print('Loading and processing old {} data...'.format(process_name))
    df_all_old = pd.DataFrame() # df_all to hold all the processed records to be bulk inserted into db
    for filename in file_old_to_process:
        if filename.endswith(".csv"):

            # if using local
            # df_temp = pd.read_csv(os.path.join(case_old_data_path, filename)) # Temporary dataframe to hold loaded csv file
            print(filename)

            # if using AWS
            df_temp = pd.read_aws_csv(os.path.join(case_old_data_path, filename))

            csv_list.append(tuple([filename, 'case', today]))
            
            ##### case data processing #####
            df_temp['date_string'] = df_temp['Last_Update'].apply(to_date_string)
            df_temp['timestamp'] = df_temp['Last_Update'].apply(to_timestamp)
            # df_temp['Confirmed'] = df_temp['Confirmed'].fillna(0.0).astype(int)
            # df_temp['Confirmed'] = df_temp['Confirmed'].fillna(np.nan).replace([np.nan], [None]) 
            # df_temp['Deaths'] = df_temp['Deaths'].fillna(np.nan).replace([np.nan], [None])
            # df_temp['Recovered'] = df_temp['Recovered'].fillna(np.nan).replace([np.nan], [None])
            # for col in df_temp.columns.to_list():
                # standardise na into None so that in insertion they would be Null
                # df_temp[col] = df_temp[col].fillna(np.nan).replace([np.nan], [None])

            df_temp['FIPS'] = ''
            df_temp['Admin2'] = ''
            df_temp['Active'] = None
            df_all_old = pd.concat([df_all_old, df_temp])

    ##### time processing #####
    df_time_old = pd.DataFrame()
    if not df_all_old.empty:
        t = df_all_old['timestamp']
        time_data = (t.dt.strftime('%Y-%m-%d %H:%M:%S'), t.dt.strftime('%Y-%m-%d'), t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
        time_col = ('Last_Update', 'date_string', 'hour', 'day', 'week', 'month', 'year', 'weekday')
        df_time_old = pd.DataFrame(dict(zip(time_col, time_data)))
        
        df_all_old = df_all_old[col_to_include]

    ##### Combine processed old and new format df together ######
    df_all_combined = pd.concat([df_all, df_all_old])
    
    df_time_combined = pd.concat([df_time, df_time_old])
               
    
    #############################
    # Bulk insert into databases
    #############################

    conn, cur = aws_util.conn_db(dbname, autoc=False)

    ##### Insert into daily_case table #####
    if not df_all_combined.empty:
        col_case_dup = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update']
        
        for col in df_all_combined.columns.to_list():
            # standardise na into None so that in insertion they would be Null
            if col in ['Confirmed', 'Deaths', 'Recovered', 'Active']:
                df_all_combined[col] = df_all_combined[col].fillna(np.nan).replace([np.nan], [None])
                df_all_combined[col] = df_all_combined[col].apply(lambda x: '{:.0f}'.format(x) if not pd.isnull(x) else x)
            elif col in col_case_dup:
                df_all_combined[col] = df_all_combined[col].fillna('')
            else:
                df_all_combined[col] = df_all_combined[col].fillna(np.nan).replace([np.nan], [None])
                # df_all_combined[col] = df_all_combined[col].fillna('')


        
        df_all_combined = df_all_combined.drop_duplicates(subset=col_case_dup, keep='first') # Drop duplicated rows, keep only the first one
        print('Bulk inserting processed {} data into {} table...'.format(process_name, process_name))
        print(df_all_combined.head())

        ##### For AWS, need to do chunk bulk insertion because AWS RDS cannot handle that many rows insertion at once #####
        n_rows = len(df_all_combined)
        chunk_size = 1000
        n_chunks = n_rows//chunk_size + 1

        cols = ','.join(list(df_all_combined.columns))
        n_col = len(df_all_combined.columns)

        for i in range(0, n_chunks):
            if (i*chunk_size) > n_rows:
                break

            if ((i+1)*chunk_size) < n_rows:
                df_chunk = df_all_combined[(i*chunk_size):((i+1)*chunk_size)]
            else:
                df_chunk = df_all_combined[(i*chunk_size):]

        
            tuples = [tuple(x) for x in df_chunk.to_numpy()]
            # Comma-separated dataframe columns
            
            # SQL quert to execute
            case_format_string = "(" + ", ".join(['%s']*n_col) + ")"
            values = [cur.mogrify(case_format_string, tup).decode('utf8') for tup in tuples]
            query  = """
            INSERT INTO daily_case ({})
            VALUES {}
            ON CONFLICT (FIPS, Admin2, Province_State, Country_Region, Last_Update)
            DO NOTHING
            """.format(cols, ",".join(values))
            
            # print(query)
            # return

            
            
            try:
                print('Start bulk inserting...')
                cur.execute(query, tuples)
                print('Successfully inserted chunk{} of {} rows'.format(i, chunk_size))
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
                return query, tuples
        
        conn.commit()
        print("Done bulk insert {} data into {} table".format(process_name, process_name))
    else:
        print('{} dataframe is empty, nothing to insert into {} table'.format(process_name, process_name))


    ##### Insert into time table #####
    if not df_time_combined.empty:
        df_time_combined = df_time_combined.drop_duplicates(subset=['Last_Update'], keep='first') # Drop duplicated rows, keep only the first one
        print('Bulk inserting into dim_time table...')

        ##### For AWS, need to do chunk bulk insertion because AWS RDS cannot handle that many rows insertion at once #####
        n_rows = len(df_time_combined)
        chunk_size = 1000
        n_chunks = n_rows//chunk_size + 1

        time_cols = ','.join(list(df_time_combined.columns))
        n_time_col = len(df_time_combined.columns)

        for i in range(0, n_chunks):
            if (i*chunk_size) > n_rows:
                break

            if ((i+1)*chunk_size) < n_rows:
                df_chunk_time = df_time_combined[(i*chunk_size):((i+1)*chunk_size)]
            else:
                df_chunk_time = df_time_combined[(i*chunk_size):]


            time_tuples = [tuple(x) for x in df_chunk_time.to_numpy()]
            # Comma-separated dataframe columns
            
            # SQL quert to execute
            # TODO: Is there a way to dynamically change the number of '%s' based on the number of columns?
            time_format_string = "(" + ", ".join(['%s']*n_time_col) + ")"
            time_values = [cur.mogrify(time_format_string, tup).decode('utf8') for tup in time_tuples]
            time_query  = """
            INSERT INTO dim_time ({})
            VALUES {}
            ON CONFLICT (Last_Update)
            DO NOTHING
            """.format(time_cols, ",".join(time_values))
            
            # print(time_query)
            
            try:
                cur.execute(time_query, time_tuples)
                print('Successfully inserted chunk{} of {} rows'.format(i, chunk_size))
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
                return time_query, time_tuples
        
        conn.commit()
        print("Done bulk insert dim_time table".format(process_name, process_name))
    else:
        print('time dataframe is empty, nothing to insert dim_time table')


    ##### Insert into csv_record table #####
    if csv_list:
        print('Bulk inserting csv records into csv_record table...')
        # print(csv_list)
        csv_record_cols_string = ','.join(list(csv_record_cols))
        csv_format_string = "(" + ", ".join(['%s']*len(csv_record_cols)) + ")"
        csv_vals = [cur.mogrify(csv_format_string, tup).decode('utf8') for tup in csv_list]
        query_csv_record = """
        INSERT INTO csv_record ({})
        VALUES {}
        ON CONFLICT (file_name, data_type)
        DO UPDATE
            SET last_update = EXCLUDED.last_update
        """.format(csv_record_cols_string, ",".join(csv_vals))
        
        # print(query_csv_record)


        try:
            cur.execute(query_csv_record, csv_list)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return query_csv_record, csv_list
        print("Done bulk insert csv records into csv_record table")
    else:
        print('no csv files were processed, nothing to insert into csv_record table')


    conn.close()

    return 'Success', 'Success'
    
    
    print("----- Successfully process {} data and insert into {} table -----".format(process_name, process_name))
    print("---------------------------------------")
    
    


def process_vaccine_data(process_all=True):
    """
    Process vaccination data
        1. For loop through the vaccination csv file
        2. Process csv files
        3. Insert the processed dataframe to database
        4. Insert the name of csv files processed into csv_record table (as a record)
       
    Args:
        conn: psycopg2 connection
        cur: psycopg2 connection cursor
        process_all: whether to process all files (True) or just the files that are not recorded in the csv_record table (False)
    
    Upsert to tables:
        vac
        csv_record
    
    Returns:
        NA
    """
    col_to_include = ['location', 'date_string', 'date', 'vaccine', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated']
    
    process_name = "vaccination"
    
    print("----- Start processing {} data -----".format(process_name))
    today = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    
    
#     vaccine_data = './vaccination_data/country_data/*.csv'
    vaccine_data_path = config['STORAGE']['VAC_DATA']

    csv_record_cols = ['file_name', 'data_type', 'last_update']


    ##############################################
    # Determine what vaccine csv files to process
    ##############################################
    ##### Get the list of vaccination csv files that have been processed before #####
    
    # If using local
    # file_all = os.listdir(vaccine_data_path) # all files in vaccination_data_path
    
    # If using AWS
    file_all = aws_util.list_files(vaccine_data_path)

    if not process_all:
        print('Determining which {} csv files to process based on csv_record table...'.format(process_name))
        sql = csv_select % ('vac')
        df_csv_record = pd.read_sql_query(sql, conn)
        file_processed_list = df_csv_record['file_name'].to_list() # files that have been recorded in db and processed before
        # print(file_processed_list)

        file_to_process = [x for x in file_all if not x in file_processed_list] # files that are not recorded in db (not processed before)
    else:
        file_to_process = file_all


    ##############################
    # Load + process vaccine data
    ##############################
    print('Loading and processing {} data...'.format(process_name))
    df_all = pd.DataFrame() # df_all to hold all the processed records to be bulk inserted into db
    csv_list = [] # csv_list to hold tuples that contain name of the file that has been processed + source file type + process date
    for filename in file_to_process:
        if filename.endswith(".csv"):

            # If using local
            # df_temp = pd.read_csv(os.path.join(vaccine_data_path, filename)) # Temporary dataframe to hold loaded csv file
            print(filename)

            # if using AWS
            df_temp = pd.read_aws_csv(os.path.join(vaccine_data_path, filename))

            csv_list.append(tuple([filename, 'vac', today]))
            

            df_temp['date_string'] = df_temp['date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d"))
            # df_temp['total_vaccinations'] = df_temp['total_vaccinations'].fillna(0.0).astype(int)
            # df_temp['people_vaccinated'] = df_temp['people_vaccinated'].fillna(0.0).astype(int)
            # df_temp['people_fully_vaccinated'] = df_temp['people_fully_vaccinated'].fillna(0.0).astype(int)
            
            
            df_temp = df_temp[col_to_include]

            df_all = pd.concat([df_all, df_temp])

            
    #############################
    # Bulk insert into databases
    #############################
    # Here is a comparison of db insertion speed.
    # https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    # The fastest is direct copy from csv. 
    # However, because we need to process the csv file before insertion, so choose the 3rd best method execute_mogrify()
    
    conn, cur = aws_util.conn_db(dbname)

    ##### Insert into vac table #####
    if not df_all.empty:
        for col in df_all.columns.to_list():
            # standardise na into None so that in insertion they would be Null
            if col in ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated']:
                df_all[col] = df_all[col].fillna(np.nan).replace([np.nan], [None])
                df_all[col] = df_all[col].apply(lambda x: '{:.0f}'.format(x) if not pd.isnull(x) else x)
            elif col in ['location', 'date_org', 'vaccine']:
                df_all[col] = df_all[col].fillna('')
            else:
                df_all[col] = df_all[col].fillna(np.nan).replace([np.nan], [None])
                
        
        df_all = df_all.rename(columns={'date': 'date_org'})
        print('Bulk inserting processed {} data into {} table...'.format(process_name, process_name))
        print(df_all.head())
        tuples = [tuple(x) for x in df_all.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df_all.columns))
        # SQL quert to execute

        vac_format_string = "(" + ", ".join(['%s']*len(df_all.columns)) + ")"
        values = [cur.mogrify(vac_format_string, tup).decode('utf8') for tup in tuples]
        query  = """
        INSERT INTO vac ({})
        VALUES {}
        ON CONFLICT (location, date_org)
        DO UPDATE SET
            vaccine = EXCLUDED.vaccine,
            total_vaccinations = EXCLUDED.total_vaccinations,
            people_vaccinated = EXCLUDED.people_vaccinated,
            people_fully_vaccinated = EXCLUDED.people_fully_vaccinated
        """.format(cols, ",".join(values))
        
        # print(query)
        
        try:
            cur.execute(query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
        print("Done bulk insert {} data into {} table".format(process_name, process_name))
    else:
        print('dataframe is empty, nothing to insert into {} table'.format(process_name))


    ##### Insert into csv_record table #####
    if csv_list:
        print('Bulk inserting csv records into csv_record table...')
        # print(csv_list)
        csv_record_cols_string = ','.join(list(csv_record_cols))
        
        csv_format_string = "(" + ", ".join(['%s']*len(csv_record_cols)) + ")"
        print(csv_format_string)
        csv_vals = [cur.mogrify(csv_format_string, tup).decode('utf8') for tup in csv_list]
        query_csv_record = """
        INSERT INTO csv_record ({})
        VALUES {}
        ON CONFLICT (file_name, data_type)
        DO UPDATE
            SET last_update = EXCLUDED.last_update
        """.format(csv_record_cols_string, ",".join(csv_vals))
        
        # print(query_csv_record)


        try:
            cur.execute(query_csv_record, csv_list)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
        print("Done bulk insert csv records into csv_record table")
    else:
        print('no csv files were processed, nothing to insert into csv_record table')

    
    conn.close()
    
    print("----- Successfully process {} data and insert into {} table -----".format(process_name, process_name))
    print("---------------------------------------")
    
    
    # return df_vaccine_table

def process_country_location():
    """
    Process vaccination data
        1. Process UID_ISO_FIPS_LookUp_Table.csv from JHU
        2. Process vaccination location lookup file from World in Data
        3. Combine/join the 2 tables
        4. Insert the combined dataframe to database
    
    The data from Johns Hopkins University (JHU) uses "Country_Region" column for countries and regions
    The data from World in Data uses "locations" column for countries and regions.
    However, the 2 may have different country/region/location names
    To resolve the problem, use the look up table from World in Data to join countries and locations from the 2 different data sources
    
    Args:
        conn: psycopg2 connection
        cur: psycopg2 connection cursor
        
    Insert to table:
        country_loc table
    
    Returns:
        NA
    """
    
    
    process_name = "country/location"
    
    print("----- Start processing {} data -----".format(process_name))
    
    process_name = "country"
    
    #####################################
    # Load country lookup table from JHU
    #####################################
#     country_path = './UID_ISO_FIPS_LookUp_Table.csv'
    country_path = config['STORAGE']['CASE_LOOKUP']
    
    
    print("Loading {} data...".format(process_name))
    
    # If using local
    # df_country = pd.read_csv(country_path)

    # If using AWS
    df_country = pd.read_aws_csv(country_path)
    
    


    print(df_country.head())
    
    ################################################
    # Load location lookup table from World in Data
    ################################################
    
    process_name = "location"
    
#     location_path = './locations.csv'
    location_path = config['STORAGE']['VAC_LOOKUP']
    
    print("Loading {} data...".format(process_name))
    
    # If using local
    # df_location = pd.read_csv(location_path)

    # IF using AWS
    df_location = pd.read_aws_csv(location_path)
    
    print("Transforming {} data...".format(process_name))
    df_location = df_location.rename(columns={'Country/Region': 'Country_Region'})

    # print(df_location)
    
    df_location = df_location[['Country_Region', 'location', 'continent']]
    # for col in df_location.columns.to_list():
    #     df_location[col] = df_location[col].fillna(np.nan).replace([np.nan], [None])

    print(df_location.head())
    
    ####################
    # Join the 2 tables
    ####################
    
    df_country_location = df_country.merge(df_location, on=["Country_Region"], how='outer')

    for col in df_country_location.columns.to_list():
        # standardise na into None so that in insertion they would be Null
        if col in ['code3', 'Population', 'UID']:
            df_country_location[col] = df_country_location[col].fillna(np.nan).replace([np.nan], [None])
            df_country_location[col] = df_country_location[col].apply(lambda x: '{:.0f}'.format(x) if not pd.isnull(x) else x)
        elif col in ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'location']:
            df_country_location[col] = df_country_location[col].fillna('')        
        else:
            df_country_location[col] = df_country_location[col].fillna(np.nan).replace([np.nan], [None])
            
    
    
    ##### Insert into vac table #####

    conn, cur = aws_util.conn_db(dbname)

    process_name = 'country_loc'
    if not df_country_location.empty:
        print('Inserting processed {} data into {} table...'.format(process_name, process_name))
        tuples = [tuple(x) for x in df_country_location.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df_country_location.columns))
        # SQL quert to execute
        country_format_string = "(" + ", ".join(['%s']*len(df_country_location.columns)) + ")"
        values = [cur.mogrify(country_format_string, tup).decode('utf8') for tup in tuples]
        query  = """
        INSERT INTO country_loc ({})
        VALUES {}
        ON CONFLICT (FIPS, Admin2, Province_State, Country_Region, location)
        DO NOTHING
        """.format(cols, ",".join(values))
        
        # print(query)
        
        try:
            cur.execute(query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
        print("Done inserting {} data into {} table".format(process_name, process_name))
    else:
        print('dataframe is empty, nothing to insert into {} table'.format(process_name))

    conn.close()

    
    
    print("----- Successfully process {} data and insert into {} table -----".format(process_name, process_name))
    print("---------------------------------------")
    
    # return df_country_location_table


############################
############################
#
# Data quality checking part
#
############################
############################

# For daily_case table
# check if the file csv dates match with the date_string values
# check if the country in the lookup table is similar to the country values
# Also for dim_time table, in the same function, use csv file dates to check against dimt_time dates

def check_csv_file_record():
    '''
    Check all the csv files available against the ones processed and recorded in csv_record table.
    1. Get the names of the csv files available
        a. case (JHU)
        b. vaccination (World in Data)
    2. Get file_name from csv_record for case and vaccination
    3. Compare list of file names from step 1 and step 2 and check if there are any files that have not been processed, or file names that have been recorded in csv_record table but not available
    '''

    conn, cur = aws_util.conn_db(dbname)

    #### Check case files against csv_record #####
    case_data_path = config['STORAGE']['CASE_DATA']
    case_old_data_path = config['STORAGE']['CASE_DATA_OLD_FORMAT']

    # If using local
    # case_file = os.listdir(case_data_path) # all files in case_data_path

    # If using AWS
    case_file = aws_util.list_files(case_data_path)

    case_file = [x for x in case_file if '.csv' in x]
    
    # If using local
    # case_file_old = os.listdir(case_old_data_path) # all files in case_old_data_path

    case_file_old = aws_util.list_files(case_old_data_path)
    case_file_old = [x for x in case_file_old if '.csv' in x]

    case_file_all = case_file + case_file_old


    case_csv_record_sql = """
        SELECT file_name
        FROM csv_record
        WHERE data_type = 'case'
    """

    df_case_csv = pd.read_sql(case_csv_record_sql, conn)
    list_case_csv_record = df_case_csv['file_name'].to_list()

    unprocessed_case_csv_file = [x for x in case_file_all if x not in list_case_csv_record]
    missing_case_csv_file = [x for x in list_case_csv_record if x not in case_file_all]

    if not unprocessed_case_csv_file:
        print("All case csv files available have been processed and recorded in csv_record table")
    else:
        print("The following case csv files available have not been processed yet: {}".format(','.join(unprocessed_case_csv_file)))

    if not missing_case_csv_file:
        print("All case csv records in csv_record table are available as csv file")
    else:
        print("The following case csv records in csv_record table are not available as csv file: {}".format(','.join(missing_case_csv_file)))

    #### Check vaccination files against csv_record #####
    vaccine_data_path = config['STORAGE']['VAC_DATA']
    # If using local
    # vac_file = os.listdir(vaccine_data_path) # all files in vaccination_data_path

    # If using AWS
    vac_file = aws_util.list_files(vaccine_data_path)
    vac_file = [x for x in vac_file if '.csv' in x]

    vac_csv_record_sql = """
        SELECT file_name
        FROM csv_record
        WHERE data_type = 'vac'
    """

    df_vac_csv = pd.read_sql(vac_csv_record_sql, conn)
    list_vac_csv_record = df_vac_csv['file_name'].to_list()

    unprocessed_vac_csv_file = [x for x in vac_file if x not in list_vac_csv_record]
    missing_vac_csv_file = [x for x in list_vac_csv_record if x not in vac_file]

    if not unprocessed_vac_csv_file:
        print("All vac csv files available have been processed and recorded in csv_record table")
    else:
        print("The following vac csv files available have not been processed yet: {}".format(','.join(unprocessed_vac_csv_file)))

    if not missing_vac_csv_file:
        print("All vac csv records in csv_record table are available as csv file")
    else:
        print("The following vac csv records in csv_record table are not available as csv file: {}".format(','.join(missing_vac_csv_file)))

    
    conn.close()


def check_case_time_table():

    conn, cur = aws_util.conn_db(dbname)

    ##### Load country from file
    country_path = config['STORAGE']['CASE_LOOKUP']
    
    # If using local
    # df_country_file = pd.read_csv(country_path)

    # If using AWS
    df_country_file = pd.read_aws_csv(country_path)
    list_file_country = set(df_country_file['Country_Region'].to_list())
    list_file_province = set(df_country_file['Province_State'].dropna().to_list())

    ######################
    # Loading from tables
    ######################

    ##### Get case csv file names from csv_record table and transform them into date  #####
    csv_record_sql = """
        SELECT file_name
        FROM csv_record
        WHERE data_type = 'case'
    """

    df_csv_record = pd.read_sql(csv_record_sql, conn)
    df_csv_record['file_date'] = (df_csv_record['file_name'].apply(lambda x: datetime.strptime(x.split('.')[0], '%m-%d-%Y')))
    list_record_date = df_csv_record['file_date'].to_list()
    list_record_date = list_record_date + [max(list_record_date) + timedelta(days=1)] # csv file contains csv records that have date = csv file record date + 1

    ##### get case dates #####
    case_date_sql = """
        SELECT DISTINCT date_string
        FROM daily_case
    """
    df_case_date = pd.read_sql(case_date_sql, conn)
    list_case_date = df_case_date['date_string'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')).to_list()

    ##### get case countries #####
    case_country_sql = """
        SELECT DISTINCT Country_Region
        FROM daily_case
    """
    df_case_country = pd.read_sql(case_country_sql, conn)
    list_case_country = df_case_country['country_region'].to_list()

    case_province_sql = """
        SELECT DISTINCT Province_State
        FROM daily_case
    """
    df_case_province = pd.read_sql(case_province_sql, conn)
    list_case_province = df_case_province['province_state'].to_list()



    ##### Get dim_time dates #####
    time_sql = """
        SELECT DISTINCT date_string
        FROM dim_time
    """

    df_time_date = pd.read_sql(time_sql, conn)
    list_time_date = df_time_date['date_string'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')).to_list()


    #####################
    # Perform check part 
    #####################

    # case dates
    missing_case_date_from_case = [datetime.strftime(x, '%Y-%m-%d') for x in list_record_date if x not in list_case_date]
    missing_case_date_from_record = [datetime.strftime(x, '%Y-%m-%d') for x in list_case_date if x not in list_record_date]

    if not missing_case_date_from_case:
        print("All dates from csv_records are found in case table dates")
    else:
        print("The following dates from csv_records are not found in case table dates: {}".format(','.join(missing_case_date_from_case)))

    if not missing_case_date_from_record:
        print("All dates from case table are found in csv_records")
    else:
        print("The following dates from case table are not found in csv_records: {}".format(','.join(missing_case_date_from_record)))
    

    # time dates
    missing_time_date_from_time = [datetime.strftime(x, '%Y-%m-%d') for x in list_record_date if x not in list_time_date]
    missing_time_date_from_record = [datetime.strftime(x, '%Y-%m-%d') for x in list_time_date if x not in list_record_date]

    if not missing_time_date_from_time:
        print("All dates from csv_records are found in dim_time table dates")
    else:
        print("The following dates from csv_records are not found in dim_time table dates: {}".format(','.join(missing_time_date_from_time)))

    if not missing_time_date_from_record:
        print("All dates from dim_time table are found in csv_records")
    else:
        print("The following dates from dim_time table are not found in csv_records: {}".format(','.join(missing_time_date_from_record)))
    

    # case countries and provinces
    missing_country_from_case = [x for x in list_file_country if x not in list_case_country]

    missing_province_from_case = [x for x in list_file_province if x not in list_case_province]

    if not missing_country_from_case:
        print("All regions from case lookup file are found in case table regions")
    else:
        print("The following regions from case lookup file are not found in case table regions: {}".format(','.join(missing_country_from_case)))

    if not missing_province_from_case:
        print("All provinces from case lookup file are found in case table provinces")
    else:
        print("The following provinces from case lookup file are not found in case table provinces: {}".format(','.join(missing_province_from_case)))

    conn.close()
    


# For vac table
# check if the file csv country match with the location values
# Get the date from vaccinations.csv (compiled by the provider that aggregates all countries together) and check against date_string values

def check_vac_table():
    conn, cur = aws_util.conn_db(dbname)

    vac_agg_path = config['STORAGE']['VAC_AGG']

    # If using local
    # df_vac_file = pd.read_csv(vac_agg_path)

    # If using AWS
    df_vac_file = pd.read_aws_csv(vac_agg_path)
    list_file_loc = set(df_vac_file['location'])
    list_file_date = set(df_vac_file['date'])

    ##### Load file_name (location) from csv_record #####
    country_loc_loc_sql = """
        SELECT DISTINCT file_name
        FROM csv_record
        WHERE data_type = 'vac'
    """
    df_country_loc_loc = pd.read_sql(country_loc_loc_sql, conn)
    df_country_loc_loc['file_name'] = df_country_loc_loc['file_name'].apply(lambda x: x.split('.')[0])
    list_country_loc_loc = df_country_loc_loc['file_name'].to_list()


    ##### Load location from vac #####
    vac_loc_sql = """
        SELECT DISTINCT location
        FROM vac
    """
    df_vac_loc = pd.read_sql(vac_loc_sql, conn)
    list_vac_loc = df_vac_loc['location'].to_list()

    ##### Load date from vac #####
    vac_date_sql = """
        SELECT DISTINCT date_string
        FROM vac
    """
    df_vac_date = pd.read_sql(vac_date_sql, conn)
    list_vac_date = df_vac_date['date_string'].to_list()

    # print(list_country_loc_loc)
    # print(list_vac_loc)
    # print(list_vac_date)

    #####################
    # Perform check part 
    #####################
    # vac locations
    missing_loc_from_vac = [x for x in list_file_loc if x not in list_vac_loc]

    if not missing_loc_from_vac:
        print("All locations from vac agg file are found in vac table locations")
    else:
        print("The following locations from vac agg lookup file are not found in vac table locations: {}".format(','.join(missing_loc_from_vac)))

    # csv_record location
    missing_loc_from_csv_record = [x for x in list_file_loc if x not in list_file_loc]

    if not missing_loc_from_csv_record:
        print("All locations from vac agg file are found in csv_record table file_name")
    else:
        print("The following locations from vac agg lookup file are not found in csv_record table file_name: {}".format(','.join(missing_loc_from_csv_record)))

    # vac date
    missing_date_from_vac = [x for x in list_file_date if x not in list_vac_date]

    if not missing_date_from_vac:
        print("All dates from vac agg file are found in vac table date_string")
    else:
        print("The following dates from vac agg lookup file are not found in vac table date_string: {}".format(','.join(missing_date_from_vac)))

    
    conn.close()



# For country_loc table
# can simply check against the csv file (since there are only 2 files)
def check_country_loc_table():

    conn, cur = aws_util.conn_db(dbname)

    ##### Load country from file #####
    country_path = config['STORAGE']['CASE_LOOKUP']
    
    # If using local
    # df_country_file = pd.read_csv(country_path)

    # If using AWS
    df_country_file = pd.read_aws_csv(country_path)

    list_file_country = set(df_country_file['Country_Region'].to_list())
    list_file_province = set(df_country_file['Province_State'].dropna().to_list())

    ##### Load location from file #####
    location_path = config['STORAGE']['VAC_LOOKUP']
    
    # If using local
    # df_location = pd.read_csv(location_path)

    # If using AWS
    df_location = pd.read_aws_csv(location_path)

    list_file_loc = set(df_location['location'].to_list())

    ######################
    # Loading from tables
    ######################

    ##### get country_loc countries #####
    country_loc_country_sql = """
        SELECT DISTINCT Country_Region
        FROM country_loc
    """
    df_country_loc_country = pd.read_sql(country_loc_country_sql, conn)
    list_country_loc_country = df_country_loc_country['country_region'].to_list()

    country_loc_province_sql = """
        SELECT DISTINCT Province_State
        FROM country_loc
    """
    df_country_loc_province = pd.read_sql(country_loc_province_sql, conn)
    list_country_loc_province = df_country_loc_province['province_state'].to_list()

    ##### get locations #####
    country_loc_loc_sql = """
        SELECT DISTINCT location
        FROM country_loc
    """
    df_country_loc_loc = pd.read_sql(country_loc_loc_sql, conn)
    list_country_loc_loc = df_country_loc_loc['location'].to_list()

    
    # print(list_country_loc_country)
    # print(list_country_loc_province)
    # print(list_country_loc_loc)
    #####################
    # Perform check part 
    #####################
    # country_loc countries and provinces
    missing_country_from_country_loc = [x for x in list_file_country if x not in list_country_loc_country]

    missing_province_from_country_loc = [x for x in list_file_province if x not in list_country_loc_province]

    if not missing_country_from_country_loc:
        print("All regions from case lookup file are found in country_loc table regions")
    else:
        print("The following regions from case lookup file are not found in country_loc table regions: {}".format(','.join(missing_country_from_country_loc)))

    if not missing_province_from_country_loc:
        print("All provinces from case lookup file are found in country_loc table provinces")
    else:
        print("The following provinces from case lookup file are not found in country_loc table provinces: {}".format(','.join(missing_province_from_country_loc)))

    
    # country_loc location
    missing_loc_from_country_loc = [x for x in list_file_loc if x not in list_country_loc_loc]

    if not missing_loc_from_country_loc:
        print("All locations from vac location lookup file are found in country_loc table locations")
    else:
        print("The following locations from vac location lookup file are not found in country_loc table locations: {}".format(','.join(missing_loc_from_country_loc)))

    conn.close()


############################
# Miscellaneous functions
############################
def to_date_string(x):
#         print(x)
    if x is None:
        return x
    elif (not '/' in x) and (not 'T' in x):
        return datetime.strptime(x.split(' ')[0], "%Y-%m-%d").strftime("%Y-%m-%d")
    elif (not '/' in x):
        return datetime.strptime(x.split('T')[0], "%Y-%m-%d").strftime("%Y-%m-%d")
    elif (len(x.split(' ')[0].split('/')[2]) == 4):
        return datetime.strptime(x.split(' ')[0], "%m/%d/%Y").strftime("%Y-%m-%d")
    else:
        return datetime.strptime(x.split(' ')[0], "%m/%d/%y").strftime("%Y-%m-%d")
    
    
def to_timestamp(x):
    # This is for creating time_table
    if x is None:
        return x
    elif (not '/' in x) and (not 'T' in x) and (len(x.split(':')) == 3):
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    elif (not '/' in x) and (not 'T' in x):
        return datetime.strptime(x, '%Y-%m-%d %H:%M')
    elif (not '/' in x) and (len(x.split(':')) == 3):
        return datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')
    elif (not '/' in x):
        return datetime.strptime(x, '%Y-%m-%dT%H:%M')
    elif (len(x.split(' ')[0].split('/')[2]) == 4):
        return datetime.strptime(x, '%m/%d/%Y %H:%M')
    else:
        return datetime.strptime(x, '%m/%d/%y %H:%M')
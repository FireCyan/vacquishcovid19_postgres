# vacquishcovid19_postgres

 * [Project overview](#Project-overview)
 * [Installation](#Installation)
 * [Quick start](#Quick-start)
 * [Source data location and outline](#Source-data-location-and-outline)
 * [DB schema and data dictionary](#DB-schema-and-data-dictionary)
 * [Data cleaning](#Data-cleaning)
 * [Data check](#Data-check)
 * [ETL pipeline](#ETL-pipeline)

## Project overview
The project aims to show the relationship between the number of daily Covid-19 cases and the number of people who have received vaccination by visualisation.

The project processes 2 data sources, one from the Center For Systems Science and Engineering at Johns Hopkins University and the other from a scientific online publication platform Our World in Data.

The dashboard is hosted on AWS EC2 and can be viewed at [www.vacquishcovid19.com](http://www.vacquishcovid19.com). The site updates its DB from the source data and the dashboard at 11am and 9pm Sydney Time (GMT+10)

These 2 data sources have been uploaded and updated to GitHub
- [source data from CSSE at JHU](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)
- [source data from Our World in Data](https://github.com/owid/covid-19-data/tree/master/public/data)
---

## Installation
```
$ pip install -r requirements.txt
```
Or use conda install if using Anaconda
```
$ conda install --file requirements.txt
```
---
 
## Quick start
The work can be roughly divided into 3 parts
1. Data warehouse creation
2. Data analysis and visualisation
3. Hosting dashboards

One should be able to do 1 and 2 with the code provided here, using the `main_script.py` script file. 

For hosting the Dash app, check the folder `covid_app`. One would need to set up AWS EC2 to host it, or can just set it up on localhost.

Note that the database used is AWS RDS Postgres. It is possible to use local Postgres for building the warehouse, one just needs to install postgres and change the connection parameters to one's local host (i.e., adapt the code in aws_util.py)

### Part 1: Data warehouse
Check the Jupyter Notebook `covid19_aws_create_datawarehouse.ipynb` for setting up the data warehouse of Covid-19 cases and vaccination.

### part 2: Data analysis and visualisation
Check the Jupyter Notebook `covid19_query_process_and_plot` for analysis and visualisation

### Part 3: Hosting dashboards
Chech the `covid_app` folder

---

## Source data location and outline

* Daily cases (new format)
    * File naming convention: MM-DD-YYYY.csv
    * Github location: COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv (03-22-2020.csv to 03-13-2021.csv)
    * Workspace location:  /covid-19-vaccination/daily_case_data/*.csv
    * Column descriptions: refer to README from [source data from CSSE at JHU](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

* Daily cases (old format)
    * File naming convention: MM-DD-YYYY.csv
    * Github location: COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv (01-22-2020.csv to 03-21-2020.csv)
    * Workspace location:  /covid-19-vaccination/daily_case_data/*.csv
    * Column descriptions: refer to README from [source data from CSSE at JHU](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data) (note only Province_State, Country_Region, Last_Update, Confirmed, Deaths and Recovered available for old format)


* Daily case UID_ISO_FIPS lookup table
    * Github location: COVID-19/csse_covid_19_data/ UID_ISO_FIPS_LookUp_Table.csv
    * workspace location: /covid-19-vaccination/UID_ISO_FIPS_LookUp_Table.csv    

* Vaccination 
    * File naming convention: Country.csv (first letter in uppercase)
    * Github location: covid-19-data_worldindata/public/data/vaccinations/country_data/*.csv
    * workspace location: /covid-19-vaccination/vaccination_data/country_data/*.csv
    * Column descriptions
        * location: Geographical location
        * date: Date of observation
        * vaccine: Name of the vaccine
        * source_url: web location of our (World in Data) source. 
        * total_vaccinations: Total number of COVID-19 vaccination doses administered
        * people_vaccinated: Total number of people who received at least one vaccine dose
        * people_fully_vaccinated: Total number of people who received all doses prescribed by the vaccination protocol

* Vaccination country location lookup table
    * Github location: covid-19-data_worldindata/public/data/vaccinations/locations.csv
    * Workspace location: /covid-19-vaccination/locations.csv

---


## DB schema and data dictionary

**Fact table: daily_case**

Number of Covid-19 cases for each day and different countries

|column name    |column type    |column description|
| ------------- |:------------- |:-----|
|FIPS           |String         |US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.|
|Admin2         |String         |County name. US only.|
|Province_State |String         |Province, state or dependency name.|
|Country_Region |String         |Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.|
|date_string    |String         |Last_Update in YYYY-MM-DD format
|Last_Update    |String         |MM/DD/YYYY HH:mm:ss (24 hour format, in UTC).|
|Confirmed      |Integer        |Counts include confirmed and probable (where reported).|
|Deaths         |Integer        |Counts include confirmed and probable (where reported).|
|Recovered      |Integer        |Recovered cases are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from COVID Tracking Project.|
|Active         |Integer        |Active cases = total cases - total recovered - total deaths.|


**Fact table: vac**

Number of vaccination against Covid-19 administered for different locations

|column name    |column type    |column description|
| ------------- |:------------- |:-----|
|location        |String         |Geographical location|
|date_string     |String         |date in YYYY-MM-DD format|
|date_org        |String         |Date of observation|
|vaccine         |String         |Name of the vaccine|
|total_vaccinations  |Integer    |total_vaccinations: Total number of COVID-19 vaccination doses administered|
|people_vaccinated   |Integer    |Total number of people who received at least one vaccine dose|
|people_fully_vaccinated |Integer|Total number of people who received all doses prescribed by the vaccination protocol|

**Dim table: country_loc**

Lookup table to join daily_case and vac tables based on column 'Country_Region' and 'location' respectively

|column name    |column type    |column description|
| ------------- |:------------- |:-----|
|Country_Region |String         |Used in case_table. Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.|
|location       |String         |Used in vaccination_table. Geographical location|
|UID            |Integer        |Unique Identifier for each row entry
|iso2           |String         |ISO 3166-1 alpha-2 code
|iso3           |String         |Officialy assigned country code identifiers
|code3          |Integer        |(No info)
|FIPS           |String         |US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.|
|Admin2         |String         |County name. US only.|
|Province_State |String         |Province, state or dependency name.|
|Combined_Key   |String         |Province_State + Country_Region
|lat            |Real           |Latitude of the country/region centroid
|long_          |Real           |Longitude of the country/region centroid
|population     |Integer        |Population of the country/region
|continent      |String         |Continent for the country/location

**Dim table: dim_time**

Time table. The data is extracted from case data source

|column name    |column type    |column description|
| ------------- |:------------- |:-----|
|Last_Update    |String         |MM/DD/YYYY HH:mm:ss (24 hour format, in UTC).|
|date_string    |String         |date in YYYY-MM-DD format|
|hour           |Integer        |hour of Last_Update
|day            |Integer        |day (of the month) of Last_Update
|week           |Integer        |week (of the year) of Last_Update
|month          |Integer        |month (of the year) of Last_Update
|year           |Integer        |year of Last_Update
|weekday        |Integer        |day (of the week) of Last_Update

**Record table: csv_record**

This table is to keep a record of which csv file has been processed and the last updated date

|column name    |column type    |column description|
| ------------- |:------------- |:-----|
|file_name      |String         |Name of the the csv file being processed|
|data_type      |String         |Whether the csv file is for daily_case or vaccination|
|last_update    |String         |The last update date in YYYY-MM-DD format|


- The 2 Dim tables are shared among the 2 Fact tables.
- The foreign key of daily_case to join country_loca table is 'Country_Region'
- The foreign key of vac to join country_loc table is 'location'

---

## Data cleaning
There are some data cleaning steps that need to be implemented because of some inconsistencies in the csv files.
These inconsistencies and the data cleaning steps are summarised below

* Daily case data
    * For csv files from 01-22-2020.csv to 03-21-2020.csv, there are some missing columns and some column names are different compared to csv files after 03-22-2020.csv
        * These files are saved in a separate directory (./daily_cases_data/old_format) because the files need to be pre-processed
        * The column naming problem is addressed by over-writing the files with function preprocess_case_data() to rename column names
        * the missing columns are added with None values in process_case_data()

    * datetime values are not consistent, and they are in one of the 6 possible formats
        * %Y-%m-%d %H:%M:%S
        * %Y-%m-%d %H:%M
        * %Y-%m-%dT%H:%M:%S
        * %Y-%m-%dT%H:%M
        * %m/%d/%y %H:%M
        * %m/%d/%Y %H:%M (Note the capital Y for 4 digit years)
        * This problem is addressed by a function with different cases (in if elif form) to standardise date format and for timestamp

    * (For Spark version that I did for Capstone project, not for this Postgres project) The columns Confirmed, Deaths, Recovered and Active should be integers. However, when loading with IntegerType() in Spark, the values for these columns would become Null
        * This is resolved by loading them as DoubleType()
        * When querying to form the table, these columns are cast to integer (cast(col AS INT) as col)

    * Due to missing columns for old format csv files, the old format and new format are loaded separately to dfs
        * The final result thus combine the 2 tables using pd.concat()
    
    * There are quite a few duplicate rows in different csv files, resulting in duplicate rows in the formed table
        * This is addressed by using .dropDuplicates() method at the end with the subset columns ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update']

    

* Vaccination data
    * The data is quite consistent
    * The only processing requried is to format date into %Y-%m-%d as string (and the column is named as date_string) so that this column can be used for joining vaccination_table with case_table

* Daily case UID_ISO_FIPS lookup table and vaccination country location lookup table
    * The data from John Hopkins University (JHU) uses "Country_Region" column for countries and regions while the data from World in Data uses "locations" column for countries and regions.
    However, the 2 data sources may have different country/region/location names.
        * To resolve the problem, join the daily case UID_ISO_FIPS lookup table and vaccination country location lookup table to form a country/location lookup table. 
        * This lookup table can then used to help join the daily case table and the vaccination table on country/region/location

---

## Data check
There are 4 functions that perform data update check (1 to check daily_case and dim_time tables, 1 to check vac table, 1 to check country_loca table and 1 to check csv_record table).

The logic for these checks I used is the same for these functions, which is checking if the csv files recorded in csv_record table have their files name (dates used for daily_case data, country for vaccination data) found in the daily_case or vac tables. For country_loc table, just use the original csv file to check if all the countries/locations are found in the country_loc table.

---

## ETL pipeline
1. Upload case and vaccination data from sources to corresponding folders in workspace
2. Create DB and tables
3. Load csv files (Extraction; case, vaccination and 2 lookup csv files), process them (Transform) and then insert (Load) them into tables
5. Perform data analysis and visualisation 

---

## Issue and solution

- 2021-05-26: There is an retrospective update on France's total case on 2021-05-25 (JHU date) where there was 'patch france data from 05-20-2021 to 05-23-2021'
    - This requires:
    1. Downloading the new csv patch files from JHU (from 2021-01-01 to 2021-05-28)
    2. Re-process these csv files and update them in database
    3. When doing rolling average, get rid of negative daily cases (retrospective update) for averaging
- 2021-06-01: There is an unusual increase in US daily cases
    - Issue: The problem is due to Nebraska cases' date being timestamped as 2021-06-01 in 06-01-2021.csv file. Usually the timestamp date is 1 day ahead of the file's date (i.e., the Nebraska date should be 2021-06-02)
    - Solution:
    1. Add a function called misc_processing to change the Last_Update from '2021-06-01' to '2021-06-02' when processing 06-01-2021.csv file in etl.py
    2. Delete old record in 2021-06-01 for US Nebraska (extended a parameter to delete records for specific country and dates in process_case_data() in etl.py)
    3. Re-process and update 2021-06-01 records for US Nebraska
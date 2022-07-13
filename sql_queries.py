##############
# DROP TABLES
##############

case_table_drop = "DROP TABLE IF EXISTS daily_case"
vac_table_drop = "DROP TABLE IF EXISTS vac"
country_loc_table_drop = "DROP TABLE IF EXISTS country_loc"
time_table_drop = "DROP TABLE IF EXISTS dim_time"


################
# CREATE TABLES
################

# Note that date_string date and Last_Update date are not the same thing
# One would be the date for the covid case reporting
# The other is the last time when the number was updated
case_table_create = ("""
CREATE TABLE IF NOT EXISTS daily_case (
    FIPS text,
    Admin2 text,
    Province_State text,
    Country_Region text,
    date_case date,
    date_string text,
    Last_Update text,
    Confirmed bigint,
    Deaths bigint,
    Recovered bigint,
    Active bigint
)
""")

vac_table_create = ("""
CREATE TABLE IF NOT EXISTS vac (
    location text,
    date_string text,
    date_org text,
    vaccine text,
    total_vaccinations bigint,
    people_vaccinated bigint,
    people_fully_vaccinated bigint
)
""")

country_loc_table_create = ("""
CREATE TABLE IF NOT EXISTS country_loc (
    Country_Region text,
    location text,
    UID int,
    iso2 text,
    iso3 text,
    code3 int,
    FIPS text,
    Admin2 text,
    Province_State text,
    Combined_Key text,
    Lat real,
    Long_ real,
    population int,
    continent text
)
""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    Last_Update text PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# csv table is to keep a record of which csv files have been processed so that we can have the option to update only the new csv files
csv_table_create = ("""
CREATE TABLE IF NOT EXISTS csv_record (
    file_name text,
    data_type text,
    last_update text
)
""")

################################################
# Add unique index for row conflict detection
# https://stackoverflow.com/questions/35888012/use-multiple-conflict-target-in-on-conflict-clause
################################################

case_unique_index_create = ("""
create unique index id_case on daily_case (FIPS, Admin2, Province_State, Country_Region, date_string);
""")

vac_unique_index_create = ("""
create unique index id_loc_date on vac (location, date_org);
""")

country_loc_unique_index_create = ("""
create unique index id_country_loc on country_loc (FIPS, Admin2, Province_State, Country_Region, location);
""")

csv_unique_index_create = ("""
create unique index id_file_data on csv_record (file_name, data_type);
""")


##################
# INSERT RECORDS
##################

case_table_insert = ("""
INSERT INTO daily_case
(FIPS, Admin2, Province_State, Country_Region, date_string, date_case, Last_Update, Confirmed, Deaths, Recovered, Active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (FIPS, Admin2, Province_State, Country_Region, Last_Update)
DO NOTHING
""")

# ON CONFLICT (songplay_id)
# DO UPDATE
#        SET FIPS = EXCLUDED.FIPS,
#            Admin2 = EXCLUDED.Admin2,
#            Province_State = EXCLUDED.Province_State,
#            Country_Region = EXCLUDED.Country_Region,
#            date_string = EXCLUDED.date_string,
#            Last_Update = EXCLUDED.Last_Update,
#            Confirmed = EXCLUDED.Confirmed,
#            Deaths = EXCLUDED.Deaths,
#            Recovered = EXCLUDED.Recovered,
#            Active = EXCLUDED.Active;

vac_table_insert = ("""
INSERT INTO vac
(location, date_string, date_org, vaccine, total_vaccinations, people_vaccinated, people_fully_vaccinated) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (location, date)
DO NOTHING
""")

# ON CONFLICT (user_id)
# DO UPDATE
#        SET level = EXCLUDED.level;

# first_name = EXCLUDED.first_name,
#            last_name = EXCLUDED.last_name,
#            gender = EXCLUDED.gender,

country_loc_table_insert = ("""
INSERT INTO country_loc
(Country_Region, location, continent) VALUES (%s, %s, %s)
ON CONFLICT (Country_Region, location)
DO NOTHING
""")

# DO UPDATE
#        SET Country_Region = EXCLUDED.Country_Region,
#            location = EXCLUDED.location,
#            continent = EXCLUDED.continent;



time_table_insert = ("""
INSERT INTO dim_time
(Last_Update, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (Last_Update)
DO NOTHING
""")

# DO UPDATE
#        SET hour = EXCLUDED.hour,
#            day = EXCLUDED.day,
#            week = EXCLUDED.week,
#            month = EXCLUDED.month,
#            year = EXCLUDED.year,
#            weekday = EXCLUDED.weekday;

csv_record_table_insert = ("""
INSERT INTO csv_record
(file_name, data_type, last_update) VALUES (%s, %s, %s)
ON CONFLICT (file_name, data_type)
DO UPDATE
    SET last_update = EXCLUDED.last_update
""")

###########
# Queries
###########

song_select = ("""
SELECT song_id, s.artist_id
FROM songs s
LEFT JOIN artists a ON s.artist_id = a.artist_id
WHERE title = %s AND name = %s AND duration = %s;
""")

csv_select = ("""
SELECT file_name
FROM csv_record 
WHERE data_type = '%s';
""")


###################
# Query list/dict
###################

create_table_dict = {'case': case_table_create, 'vac': vac_table_create, 'country': country_loc_table_create, 'time': time_table_create, 'csv': csv_table_create}
create_unique_index_dict = {'case': case_unique_index_create, 'vac': vac_unique_index_create, 'country': country_loc_unique_index_create, 'csv': csv_unique_index_create}
drop_table_queries = [case_table_drop, vac_table_drop, country_loc_table_drop, time_table_drop]
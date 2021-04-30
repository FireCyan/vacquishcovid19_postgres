import psycopg2
from sql_queries import create_table_dict, create_unique_index_dict, drop_table_queries
import aws_util


def create_database(db_name):
    """
    - Creates and connects to the covid19_db
    - Returns the connection and cursor to covid19_db
    
    Args:
        NA
    
    Returns:
        conn: psycopg2 connection
        cur: psycopg2 connection cursor
    """
    
    # connect to default database
    print("--- Creating database -----")
    
    ##### If using localhost #####
    # print("connecting to Postgres default_db")
    # conn = psycopg2.connect("host=127.0.0.1 dbname=default_db user=postgres password=localtest")
    # conn.set_session(autocommit=True)
    # cur = conn.cursor()

    print("connecting to AWS RDS Postgres default_db")
    conn, cur = aws_util.conn_default()

    
    # create sparkify database with UTF8 encoding
    print("Dropping database if exists...")
    cur.execute("DROP DATABASE IF EXISTS covid19_db")
    print("Creating database {}...".format(db_name))
    cur.execute("CREATE DATABASE {} WITH ENCODING 'utf8' TEMPLATE template0".format(db_name))

    # close connection to default database
    conn.close()    
    
    ##### If using localhost #####
    # connect to covid19_db database
    # print("Connecting to database {}...".format(db_name))
    # conn = psycopg2.connect("host=127.0.0.1 dbname=covid19_db user=postgres password=localtest")
    # cur = conn.cursor()

    print("Connecting to AWS RDS database {}...".format(db_name))

    conn, cur = aws_util.conn_db(db_name)

    print('--- Finish creating database ---')

    conn.close()
    
    # return cur, conn


def drop_tables(conn, cur):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Args:
        cur: psycopg2 connection cursor
        conn: psycopg2 connection
    
    Returns:
        NA
        
    """
    print('--- Dropping tables in db ---')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    print('---Finish dropping tables ---')

def create_tables(conn, cur):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Args:
        cur: psycopg2 connection cursor
        conn: psycopg2 connection
    
    Returns:
        NA
    """
    print('--- Creating tables in db ---')
    # for query in create_table_queries:
    for key, query in create_table_dict.items():
        # Create table
        cur.execute(query)
        # Create unique index
        if key != 'time':
            cur.execute(create_unique_index_dict[key])
        
        conn.commit()

    print('--- Finish creating tables ---')


def main():
    """
    - Drops (if exists) and Creates the covid19_db database.     
    - Establishes connection with the covid19_db database and gets
    cursor to it.  
    
    1. Drop all the tables    
    2. Create all tables needed    
    3. Close the connection
    """

    db_name = 'covid19_db'

    create_database(db_name)

    conn, cur = aws_util.conn_db(db_name)
    
    drop_tables(conn, cur)
    create_tables(conn, cur)

    conn.close()


if __name__ == "__main__":
    main()
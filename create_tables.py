import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop any existing tables in the database

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 dwhSparkify connection

    OUTPUT
    none
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create dwhSparkify tables

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 dwhSparkify connection

    OUTPUT
    none
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connect to the AWS Redshift data warehouse, drop any existing tables, and create or recreate the required tables; close the database connection when finished

    INPUT
    none

    OUTPUT
    none
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER', 'HOST'),
        config.get('CLUSTER', 'DWH_NAME'),
        config.get('CLUSTER', 'DWH_USER'),
        config.get('CLUSTER', 'DWH_PASSWORD'),
        config.get('CLUSTER', 'DWH_PORT')))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

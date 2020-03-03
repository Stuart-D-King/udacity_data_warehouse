import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Ingest song and log data from S3 into the song and log staging tables

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 dwhSparkify connection

    OUTPUT
    none
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert song and log data from the staging tables into the data warehouse data schema

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 dwhSparkify connection

    OUTPUT
    none
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connect to the Sparkify data warehouse, ingest song and log data into staging tables, and insert processed data into schema tabales; close the connection to the database when finished
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER', 'HOST'),
        config.get('CLUSTER', 'DWH_NAME'),
        config.get('CLUSTER', 'DWH_USER'),
        config.get('CLUSTER', 'DWH_PASSWORD'),
        config.get('CLUSTER', 'DWH_PORT')))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

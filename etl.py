import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
    This function use to loading data into staging table.
         Argument:
            cur: Database Cursor
            conn: connect to RedShift Cluster
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        # execute query
        cur.execute(query)
        # commit query
        conn.commit()

"""
    This function use to insert data into fact and dim table.
         Argument:
            cur: Database Cursor
            conn: connect to RedShift Cluster
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        # execute query
        cur.execute(query)
        # commit query
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
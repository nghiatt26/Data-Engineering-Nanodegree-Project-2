import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
    This function use to drop table of staging, fact and dim table.
         Argument:
            cur: Database Cursor
            conn: connect to RedShift Cluster
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        # execute query
        cur.execute(query)
        # commit query
        conn.commit()

"""
    This function use to create table of staging, fact and dim table.
         Argument:
            cur: Database Cursor
            conn: connect to RedShift Cluster
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        # execute query
        cur.execute(query)
        # commit query
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
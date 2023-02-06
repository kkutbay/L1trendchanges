import datetime
import sqlite3
import os

def create_connection(db_file):
    """ create a database and tables connection to a SQLite database """
    conn = None
    newdb = False
    if not os.path.exists(db_file):
        newdb = True
    try:
        conn = sqlite3.connect(db_file)
        #print(sqlite3.version)
        if newdb:
            tabels = create_tables(conn)
            print(f'newdb created:{newdb}, tables:{tabels}')
        print(f'db_file:{db_file}')
    except sqlite3.Error as e:
        print(e)
    
    return conn

def create_tables(conn):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    create_prices = """CREATE TABLE prices (
        date     TIMESTAMP,
        symbol   TEXT,
        region   TEXT,
        open     REAL,
        high     REAL,
        low      REAL,
        close    REAL,
        volume   REAL,
        updatedt TIMESTAMP
    );"""

    create_prices_pk_index ="""
        CREATE UNIQUE INDEX pk_prices ON prices (
        date,
        symbol,
        region
    );"""

    create_events = """CREATE TABLE events (
        date     TIMESTAMP,
        symbol   TEXT,
        region   TEXT,
        amount   REAL,
        type     TEXT,
        data     REAL,
        updatedt TIMESTAMP
    );
    """

    create_events_pk = """CREATE UNIQUE INDEX pk_events ON events (
        date,
        symbol,
        region,
        type
    );
    """

    try:
        c = conn.cursor()
        c.execute(create_prices)
        c.execute(create_prices_pk_index)
        c.execute(create_events)
        c.execute(create_events_pk)
        return 'prices, events'
    except sqlite3.Error as e:
        print(e)

def delete_tableentries(conn, table, symbol, region, dates):
    """
    Delete existing tableentries  
    :param conn:  Connection to the SQLite database
    :param symbol: symbol of the task
    :dates: dates list
    :return:
    """
    sql = f"DELETE FROM {table} WHERE symbol='{symbol}' and region = '{region}' and date in ({dates})"
    #print(sql)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        print(e + ' \nsql: '+sql)


def get_tables(conn):
    sql = "SELECT name FROM sqlite_schema WHERE  type ='table' AND  name NOT LIKE 'sqlite_%';"
    try:
        c = conn.cursor()
        c.execute(sql)
        rows = c.fetchall()
        return rows

    except sqlite3.Error as e:
        print(e + ' \nsql: '+sql)





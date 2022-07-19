import sqlite3

from sqlite3 import Error
from typing import final


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("SQLTE3 connection open")
    except Error as e:
        print(e)
    
    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
    finally:
        print("Closing database connection")
        conn.close()


def select_all_rows(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def main():
    database = "db/expenses.db"

    # sql_create_target_table = """ CREATE TABLE IF NOT EXISTS expenses (
    #                                     date datetime PRIMARY KEY,
    #                                     USD integer,
    #                                     rate float
    #                                 ); """

    # # create a database connection
    conn = create_connection(database)

    # # create tables
    # if conn is not None:
    #     # create table
    #     create_table(conn, sql_create_target_table)

    # else:
    #     print("Error! cannot create the database connection.")
    select_all_rows(conn)

if __name__ == '__main__':
    main()
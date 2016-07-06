import sqlite3
import os, os.path
import datetime, time

def adapt_datetime(ts):
    return time.mktime(ts.timetuple())

def create_backup_database(database_path, overwrite_existing=False):
    """
    Creates a SQLite database at *database_path* that is ready to be used as backup tracking for this software.
    :param database_path The path to the database file to create.
    :param overwrite_existing
    If a file already exists at *database_path*, should it be overwritten?
    """
    if os.path.exists(database_path):
        if overwrite_existing:
            os.remove(database_path)
        else:
            raise IOError("The file %s already exists" % database_path)

    sqlite3.register_adapter(datetime.datetime, adapt_datetime)
    connection = sqlite3.connect(database_path)

    sql_arch_tbl = '''create table archives
    (path text, vaultName text,
    treehash text, size integer,
    timestampUploaded integer, archiveID text,
    awsURI text, toDelete integer)'''
    connection.execute(sql_arch_tbl)

    sql_vault_tbl = '''create table vaults
    (arn text, name text, region text)'''
    connection.execute(sql_vault_tbl)

    return connection


def connect_to_database(database_path):
    if not os.path.exists(database_path):
        conn = create_backup_database(database_path)
    else:
        conn = sqlite3.connect(database_path)

    conn.row_factory = sqlite3.Row

    return conn

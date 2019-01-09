# -*- coding: utf-8 -*-
# Created by Wei Wang

import os
from os.path import expanduser
import datetime
from configparser import ConfigParser
import pandas as pd
import mysql.connector


# from mars.db.global_var import MYSQL_CONFIG_FILE

def _mysql_query(sql, conn, fetch=True):
    """
    Run sql with given connection.

    :param sql:
    :param conn:
    :return:
    """
    cursor = conn.cursor()

    df = None
    try:
        cursor.execute(sql)

        if fetch:
            df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

        conn.commit()  # remember to commit it
    except mysql.connector.Error as err:
        print('Caught exception: {}'.format(err))
    else:
        cursor.close()

    return df


def _mysql_get_connection(section, verbose=False):
    """
    Get MySQL connection.

    :param section:
    :return:
    """
    # print('Configuration file: %s' % MYSQL_CONFIG_FILE)

    # config = ConfigParser.RawConfigParser()
    config = ConfigParser()

    key_file = '%s/key/mysql.key.cfg' % os.path.pardir
    if verbose:
        print('key_file %s' % key_file)
    config.read(key_file)

    # home = expanduser("~")
    # config.read('%s/%s' % (home, MYSQL_CONFIG_FILE))

    # print(config.sections())
    host = config.get(section, 'host')
    port = config.get(section, 'port')
    default_db = config.get(section, 'default_db')
    username = config.get(section, 'username')
    password = config.get(section, 'password')

    conn = mysql.connector.connect(
        user=username,
        password=password,
        host=host,
        port=port,
        database=default_db,
        charset='utf8'
    )
    return conn


def select_section(db, section="DB_SELECTION"):
    """
    Select a db.

    :param db: e.g. "sensor1_read", "sensor1_write"
    :return: selected db
    """
    key_file = '%s/key/mysql.key.cfg' % os.getenv(os.path.pardir) + './keys'
    config = ConfigParser()
    config.read(key_file)
    return config.get(section, db)


def mysql_query(sql, section, verbose=False):
    """
    Run sql with given section.

    :param sql:
    :param section:
    :return:
    """
    conn = _mysql_get_connection(section, verbose)
    _mysql_query(sql, conn, fetch=False)
    conn.close()


def mysql_export_data_to_df(sql, section, verbose=False):
    """
    Export data from MySQL to pandas DataFrame.

    :param sql:
    :param section:
    :return:
    """
    conn = _mysql_get_connection(section, verbose)
    try:
        df = _mysql_query(sql, conn)
    except mysql.connector.Error as err:
        print('Caught exception: {}'.format(err))
    else:
        conn.close()

    return df


def prepare_value(value):
    rtn = value
    if value is None or pd.isnull(rtn):
        rtn = 'NULL'
    elif isinstance(rtn, str):  # or isinstance(rtn, unicode):
        rtn = rtn.replace("'", '"').replace('\\', '')
        rtn = "'%s'" % rtn
    elif isinstance(rtn, datetime.date) or \
            isinstance(rtn, pd.Timestamp):
        rtn = "'%s'" % rtn
    return str(rtn)


def mysql_import_data_from_df_batch(
        df, table_name, section,
        batch_size=10000,
        replace=False,
        verbose=False
):
    """
    Batch import for huge data frame.

    :param df:
    :param table_name:
    :param section:
    :param batch_size:
    :return:
    """
    num_total = len(df)
    num_batch = int(1 + num_total / batch_size)
    print('Batch settings: num_total=%d, batch_size=%d, num_batch=%d' %
          (num_total, batch_size, num_batch))
    for i in range(num_batch):
        begin_index = batch_size * i
        end_index = batch_size * (i + 1) - 1
        if end_index > num_total - 1:
            end_index = num_total - 1
        if begin_index > end_index:
            begin_index = end_index
        print('Batch %d: %d - %d' % (i, begin_index, end_index))
        df_batch = df.ix[begin_index:end_index]
        mysql_import_data_from_df(
            df_batch, table_name, section,
            replace=replace,
            verbose=verbose
        )


def mysql_import_data_from_df(
        df, table_name, section, verbose=False, replace=False
):
    """
    Import data from pandas DataFrame.

    :param df:
    :param table_name:
    :param section:
    :return:
    """
    # print(df.head())

    if len(df) == 0:
        print("mysql_import_data_from_df: empty data: %s, %s." %
              (table_name, section))
        return

    list_rows = []

    def process_row(row):
        values = []
        for i in range(len(row)):
            v = row[i]
            values.append(prepare_value(v))
        list_rows.append('(%s)' % ','.join(values))

    df.apply(process_row, axis=1)

    list_columns = [('`%s`' % x) for x in df.columns]

    sql = '''
        %s into %s
        (%s)
        VALUES
        %s
    ''' % (
        'replace' if replace else 'insert',
        table_name,
        ','.join(list_columns),
        ',\n'.join(list_rows)
    )

    if verbose:
        print(sql)

    conn = _mysql_get_connection(section, verbose)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()  # remember to commit it
    except mysql.connector.Error as err:
        print('Caught exception: {}'.format(err))
    else:
        cursor.close()
        conn.close()


def mysql_import_data_from_csv_fast(input_file_path, table_name, section):
    """
    Import data from csv using "LOAD DATA INFILE".

    :param input_file_path:
    :param table_name:
    :param section:
    :return:
    """
    sql = '''
        LOAD DATA INFILE '%s'
        INTO TABLE %s
        FIELDS TERMINATED BY ','
    ''' % (input_file_path, table_name)

    conn = _mysql_get_connection(section)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print('Caught exception: {}'.foramt(err))
    else:
        cursor.close()
        conn.close()


def mysql_update_table_batch(
        table_name, table_column_key, table_column_value,
        df, df_column_key, df_column_value,
        section,
        where_clause='',
        table_column_id='ID', batch_size=10000, batch_skip=-1,
        verbose=False
):
    sql_id = '''
        select
            min(%s), max(%s)
        from
            %s
        %s
    ''' % (table_column_id, table_column_id, table_name, where_clause)
    # print(sql_id)
    df_id = mysql_export_data_to_df(sql_id, section)
    min_id = df_id.as_matrix()[0, 0]
    max_id = df_id.as_matrix()[0, 1]

    if (len(df_id) <= 0) or (min_id is None) or (max_id is None):
        print('Empty data frame')
        print(sql_id)
        return

    num_total = max_id - min_id + 1
    num_batch = int(num_total / batch_size) + 1

    print('Batch settings: num_total=%d, batch_size=%d, num_batch=%d' % (
        num_total, batch_size, num_batch
    ))

    where_clause_main = where_clause.strip()
    if len(where_clause_main) > 0:
        if where_clause_main.lower().startswith('where'):
            where_clause_main = where_clause_main[5:]  # remove 'where'
        where_clause_main = 'and %s' % where_clause_main

    for i in range(num_batch):
        begin_id = min_id + i * batch_size
        end_id = begin_id + batch_size - 1
        where_clause_id = 'where %s >= %d and %s <= %d %s' % (
            table_column_id, begin_id, table_column_id, end_id,
            where_clause_main
        )
        print('Batch %d/%d: %d - %d' % (i, num_batch, begin_id, end_id))
        if i <= batch_skip:
            print('Skip')
            continue

        mysql_update_table(
            table_name, table_column_key, table_column_value,
            df, df_column_key, df_column_value,
            section,
            where_clause=where_clause_id,
            verbose=verbose
        )


def mysql_update_table_list(
        table_name, table_column_key, table_column_value_list,
        df, df_column_key, df_column_value_list,
        section,
        where_clause='',
        verbose=False
):
    for i in range(len(table_column_value_list)):
        v1 = table_column_value_list[i]
        v2 = df_column_value_list[i]
        print('Updating %s <- %s' % (v1, v2))
        mysql_update_table(
            table_name, table_column_key, v1,
            df, df_column_key, v2,
            section,
            where_clause,
            verbose
        )


def mysql_update_table(
        table_name, table_column_key, table_column_value,
        df, df_column_key, df_column_value,
        section,
        where_clause='',
        verbose=False
):
    """
    update table_name
    set table_column_value = df_column_value
    when table_column_key equals to df_column_key

    :param table_name:
    :param table_column_key:
    :param table_column_value:
    :param df:
    :param df_column_key:
    :param df_column_value:
    :param section:
    :param where_clause: e.g. 'where CITY_ID=1'
    :return:
    """
    sql_list = [
        'UPDATE %s' % table_name,
        'SET %s = CASE' % table_column_value
    ]

    def process_row(row):
        sql_list.append('    WHEN %s="%s" THEN %s' % (
            table_column_key,
            row[df_column_key],
            prepare_value(row[df_column_value])
        ))

    if len(df) == 0:
        print("Empty data frame.")
        return

    df.apply(lambda row: process_row(row), axis=1)
    sql_list.append('ELSE %s' % table_column_value)
    sql_list.append('END')
    sql_list.append(where_clause)
    sql = '\n'.join(sql_list)
    # print(sql)

    conn = _mysql_get_connection(section, verbose)
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        conn.commit()  # remember to commit it
    except mysql.connector.Error as err:
        print('Caught exception: {}'.format(err))
    else:
        cursor.close()
        conn.close()

    return sql


def main():
    home = expanduser("~")
    print(home)

    # Examples
    sql = '''
        SELECT * FROM MEASURE_POINT
    '''
    print(sql)

    SECTION_PROD_SENSOR1 = 'PROD_SENSOR1'
    df = mysql_export_data_to_df(sql, SECTION_PROD_SENSOR1)
    print(df.head())
    print('Done')


if __name__ == '__main__':
    main()
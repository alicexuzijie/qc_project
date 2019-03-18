# -*- coding: utf-8 -*-
import sys
import numpy as np
import pandas as pd
import common
import utility.utility as utility
from utility import time_utility as tu


def compose_sensor_query_statement(hour, city_list=None, device_list=None):
    """

    :param city_list:
    :param device_list:
    :return:
    """
    city_str = ''
    if city_list:
        city_str = 'AND MP.CITYID IN ('
        city_str += utility.int_list_to_str(city_list)
        city_str += ') '

    device_str = ''
    if device_list:
        device_str = 'AND SENSOR_ID IN ('
        device_str += utility.str_list_to_str(device_list)
        device_str += ')'

    sql_statement = '''
  SELECT 
    UCASE(S_INFO.SENSOR_ID) AS DEV_ID,
    S_INFO.SEN_DEV_TYPE_ID,
    S_INFO.MEASURE_POINT_ID,
    S_INFO.RELATE_SITE_ID,
    VAR_GROUP.VARGROUP_ID,
    S_INFO.LATITUDE,
    S_INFO.LONGITUDE,
    S_INFO.ALTITUDE,
    S_INFO.CITYID,
    VAR_GROUP.ABBR_CODE
    FROM
        (SELECT 
            SI.SENSOR_ID,
                SI.SEN_DEV_TYPE_ID,
                SI.MEASURE_POINT_ID,
                MP.RELATE_SITE_ID,
                MP.GOOGLELATITUDE as LATITUDE,
                MP.GOOGLELONGITUDE as LONGITUDE,
                MP.CITYID,
                MP.ALTITUDE
        FROM
            SENSOR_INFO SI, MEASURE_POINT MP
        WHERE
            SI.SENSOR_ID NOT IN (SELECT DISTINCT
                    SENSOR_ID
                FROM
                    T_ABNORM_DEVICE
                where TIMESTAMP = '{}')
                AND SI.MEASURE_POINT_ID = MP.ID
                AND  SI.STATE>'1000'
                AND  SI.STATE<'2000'
                AND  SI.istestsensor=0
                AND  MP.testpointflag=0
                AND  CITYID>0
                {}
                {}
    ) S_INFO,
        (SELECT
            SENSOR_ID, VARGROUP_ID, ABBR_CODE
        FROM
            T_SENSOR_VARGROUP_MAP
            WHERE VARGROUP_ID IS NOT NULL ) VAR_GROUP
    WHERE
        S_INFO.SENSOR_ID = VAR_GROUP.SENSOR_ID
        AND S_INFO.SENSOR_ID NOT LIKE "YSRDVHP3ZT%"
        AND S_INFO.SENSOR_ID NOT LIKE "YSRDVHP7PT%"

    '''.format(hour, city_str, device_str)

    return sql_statement


def compose_version_query_statement():
    sql_statement = '''
    SELECT 
        VARGROUP_ID,
        VAR_TYPE_ID,
        QUALITYCONTROL_VERSION
    FROM
        T_SENSOR_VARGROUP_DETAIL_INFO 
    ORDER BY 
        VARGROUP_ID
    '''

    return sql_statement


def compose_transducerlist_query_statement():
    sql_statement = '''
    SELECT 
        VARGROUP_ID,
        VAR_TYPE_ID,
        TRANSDUCERLIST AS CHANNEL_LIST
    FROM
        T_SENSOR_VARGROUP_DETAIL_INFO 
    ORDER BY 
        VARGROUP_ID
    '''

    return sql_statement


def compose_field_name_statement(field_tuple):
    sql_statement = '''SELECT DEV_TYPE_ID, GROUP_CONCAT(COL_NAME) AS COL_NAME FROM DEVICE_COLUMN_TYPE 
          WHERE VAR_TYPE_ID in (SELECT VAR_TYPE_ID FROM DEVICE_CAPTURE_VALUE_TYPE 
          WHERE VAR_TYPE_NAME in {}) GROUP BY DEV_TYPE_ID'''.format(field_tuple)

    return sql_statement


def compose_consistency_model_query_statement():
    sql_statement = """
           SELECT DEVICE_ID as DEV_ID ,VAR as CHANNEL ,ARG1 ,ARG2 from T_SENSOR_DEVICE_NORMALIZATION_MODEL
           """
    return sql_statement


def compose_capture_data_query_statement(field_name, sen_dev_type_id, device_list, starttime, endtime):
    """

    :param sen_dev_type_id:
    :param city_list:
    :param device_list:
    :return:
    starttime = tu.datetime_n_hours_before_string(
            tu.time_str_to_datetime(endtime), 1)
        endtime = starttime[0:13] + ':59:59'
    """
    device_str = ''
    if device_list:
        device_str = 'DEV_ID IN ('
        device_str += utility.str_list_to_str(device_list)
        device_str += ')'

    if sen_dev_type_id == 1:
        sen_dev_type_id = 2

    if sen_dev_type_id in [2, 3, 7]:
        if starttime[5:7] == endtime[5:7]:
            table = 'DEVICE_CAPTURE_DATA_' + \
                    str(sen_dev_type_id) + "_" + endtime[:4] + endtime[5:7]
            sql_statement = """
                        SELECT
                            DEV_ID,
                            MEA_POINT_ID,
                            {4} 
                            ,CAP_TIME
                        FROM
                            {3}
                        WHERE
                            {0}
                        AND CAP_TIME >= '{1}'
                        AND CAP_TIME <= '{2}'
                        AND MEA_POINT_ID > 0        
            """.format(device_str, starttime, endtime, table, field_name)
        else:
            last_table = 'DEVICE_CAPTURE_DATA_' + \
                    str(sen_dev_type_id) + "_" + starttime[:4] + starttime[5:7]
            this_table = 'DEVICE_CAPTURE_DATA_' + \
                    str(sen_dev_type_id) + "_" + endtime[:4] + endtime[5:7]
            sql_statement = """
                        SELECT
                            DEV_ID,
                            MEA_POINT_ID,
                            {0} 
                            ,CAP_TIME
                        FROM
                            {1}
                        WHERE
                            {2}
                        AND CAP_TIME <= '{3}'
                        AND MEA_POINT_ID > 0   

                        UNION ALL

                         SELECT
                            DEV_ID,
                            MEA_POINT_ID,
                            {4} 
                            ,CAP_TIME
                        FROM
                            {5}
                        WHERE
                            {6}
                        AND CAP_TIME >= '{7}'
                        AND MEA_POINT_ID > 0   

            """.format(field_name, this_table, device_str, endtime, field_name,last_table, device_str, starttime)

        # print(sql_statement)
        return sql_statement


def compose_org_data_query_statement(sen_dev_type_id, device_list, endtime, starttime=None):
    device_str = ''
    if device_list:
        device_str = 'DEV_ID IN ('
        device_str += utility.str_list_to_str(device_list)
        device_str += ')'
    if starttime is None:
        starttime = endtime
    if sen_dev_type_id == 1:
        table = 'DEV_ORG_PER_HOUR_2' + "_" + starttime[:4] + starttime[5:7]
        sql_statement = """
                 SELECT
                    DEV_ID,
                    MEASURE_POINT_ID,
                    PM25,
                    PM10,
                    SO2,
                    CO,
                    NO2,
                    O3,
                    TVOC,
                    OUTSIDE_HUMIDITY,
                    OUTSIDE_TEMPERATURE,
                    CAL_TIME,
                    COUNT_PM25,
                    COUNT_PM10,
                    COUNT_SO2,
                    COUNT_CO,
                    COUNT_NO2,
                    COUNT_O3,
                    COUNT_TVOC
                FROM
                    {0}
                WHERE
                    {1}
                AND CAL_TIME >= "{2}"
                AND CAL_TIME <= "{3}"
        """.format(table, device_str, starttime, endtime)
        # print(sql_statement)
        return sql_statement
    elif sen_dev_type_id == 3:
        table = 'DEV_ORG_PER_HOUR_3' + "_" + starttime[:4] + starttime[5:7]
        sql_statement = """
                         SELECT
                            DEV_ID,
                            MEASURE_POINT_ID,
                            PM25,
                            PM10,
                            OUTSIDE_HUMIDITY,
                            OUTSIDE_TEMPERATURE,
                            CAL_TIME,
                            COUNT_PM25,
                            COUNT_PM10
                        FROM
                            {0}
                        WHERE
                            {1}
                        AND CAL_TIME >= "{2}"
                        AND CAL_TIME <= "{3}"
                """.format(table, device_str, starttime, endtime)
        # print(sql_statement)
        return sql_statement
    elif sen_dev_type_id == 7:
        table = 'DEV_ORG_PER_HOUR_7' + "_" + starttime[:4] + starttime[5:7]
        sql_statement = """
                         SELECT
                            DEV_ID,
                            MEASURE_POINT_ID,
                            TSP,
                            PM25,
                            OUTSIDE_HUMIDITY,
                            OUTSIDE_TEMPERATURE,
                            CAL_TIME,
                            COUNT_TSP,
                            COUNT_PM25
                        FROM
                            {0}
                        WHERE
                            {1}
                        AND CAL_TIME >= "{2}"
                        AND CAL_TIME <= "{3}"
                """.format(table, device_str, starttime, endtime)
        # print(sql_statement)
        return sql_statement


def convert_dic_format_for_org(org_data):
    org_data.rename(columns={'OUTSIDE_HUMIDITY': 'HUMIDITY', 'OUTSIDE_TEMPERATURE': 'TEMPERATURE', 'CAL_TIME': 'TIMESTAMP', 'RELATE_SITE_ID': 'SITE_ID'}, inplace=True)
    org_data = org_data.reindex(columns=['DEV_ID', 'PM25', 'PM10', 'SO2', 'CO', 'NO2', 'O3', 'TVOC', 'TSP', 'HUMIDITY', 'TEMPERATURE', 'TIMESTAMP', 'SITE_ID', 'COUNT_PM25', 'COUNT_PM10', 'COUNT_SO2', 'COUNT_CO', 'COUNT_NO2', 'COUNT_O3', 'COUNT_TVOC', 'COUNT_TSP', 'VARGROUP_ID'])  
    org_data = org_data.replace([None], np.nan)
    return org_data


def compose_aq_type():
    sql_statement = '''
    SELECT 
        AQ_TYPE AS VAR_TYPE_ID,
        NAME AS VAR_NAME
    FROM
        T_DICT_AQ_TYPE;
    '''
    return sql_statement


def retrieve_site_data_by_var(site_data, var_name, var_type_id):
    # print(var_type_id)
    cur_var_site_data = site_data[site_data['TYPE'] == var_type_id]
    cur_var_site_data = cur_var_site_data[['TIMESTAMP', 'SITE_ID', 'VALUE']]
    cur_var_site_data.rename(columns={'VALUE': 'SITE_{}'.format(var_name)}, inplace=True)
    cur_var_site_data = cur_var_site_data.sort_values(by=['TIMESTAMP', 'SITE_ID'])
    # print(cur_var_site_data.head())
    return cur_var_site_data


def convert_dic_format_for_site(site_data):
    site_data_dic = {}
    var_info_dict = {1: 'PM25', 2: 'PM10', 3: 'SO2', 4: 'CO', 5: 'NO2', 6: 'O3', 10: 'TSP'}
    for key, value in var_info_dict.items():
        # print (key,value)
        site_data_dic[key] = retrieve_site_data_by_var(site_data, value, key)

    site_data_wide_table = site_data_dic[1]
    # print(site_data_wide_table)
    for key, value in var_info_dict.items():
        if key == 1:
            continue
        elif key not in (1, 10):
            if (site_data_dic[key].shape[0] == 0):
                continue
            site_data_wide_table = site_data_wide_table.merge(site_data_dic[key], on=['TIMESTAMP', 'SITE_ID'], how='outer')
        elif key == 10:
            site_data_dic_10 = site_data_dic[key].reindex(columns=['TIMESTAMP', 'SITE_ID', 'SITE_PM25', 'SITE_PM10', 'SITE_SO2', 'SITE_CO', 'SITE_NO2', 'SITE_O3', 'SITE_TSP'])  
            site_data_wide_table = site_data_wide_table.append(site_data_dic_10)

    return site_data_wide_table


def compose_site_id_query_statement(city_id=None, site_id=None):
    """

    :param city_list:
    :param device_list:
    :return:
    """
    city_str = ''
    if city_id:
        city_str = 'AND CITY_ID IN ('
        city_str += utility.int_list_to_str(city_id)
        city_str += ') '

    site_str = ''
    if site_id:
        site_str = 'AND SITE_ID IN ('
        site_str += utility.int_list_to_str(site_id)
        site_str += ')'

    sql_statement = '''
    select SITE_ID,LONGITUDE,LATITUDE, CITY_ID from MONITORING_SITE where 
    LATITUDE>0 AND LONGITUDE>0 AND DELETE_FLAG = 0  AND SITE_ID IN (
                SELECT DISTINCT RELATE_SITE_ID
                FROM
                    SENSOR_INFO S,
                    MEASURE_POINT M
                WHERE
                S.MEASURE_POINT_ID = M.ID
                AND RELATE_SITE_ID > 0) {0} {1}
    '''.format(city_str, site_str)

    return sql_statement


def compose_adjust_value_query_statement(device_list, start_time, end_time, table, VAR_TYPE_ID=None):
    """

    :param city_list:
    :param device_list:
    :return:
    """
    device_str = ''
    if device_list:
        device_str = 'AND DEV_ID IN ('
        device_str += utility.int_list_to_str(device_list)
        device_str += ') '
    var_type_id_str = ''
    if VAR_TYPE_ID:
        var_type_id_str = 'AND VAR_TYPE_ID IN ('
        var_type_id_str += utility.int_list_to_str(device_list)
        var_type_id_str += ') '

    sql_statement = '''
    SELECT * from {0} WHERE ADJ_TIME>='{1}' AND ADJ_TIME<='{2}'
    {3} {4}
    '''.format(table, start_time, end_time, device_str, var_type_id_str)

    return sql_statement


def strip_column(df, column_name):
    '''
    给定列名，对某一个dataframe的该列，去掉回车号，空格
    '''
    df[column_name] = df.apply(lambda row: row[column_name].replace(' ', ''), axis=1)
    df[column_name] = df.apply(lambda row: row[column_name].replace('\n', ''), axis=1)

    return df


def append_device_info_to_df(df, sensor_dev_info, columns_to_append):

    tmp_df = sensor_dev_info[['DEV_ID'] + columns_to_append].copy()
    tmp_df.rename(columns={'DEV_ID': 'TMP_DEV_ID'}, inplace=True)
    df = df.merge(tmp_df, left_on='DEV_ID', right_on='TMP_DEV_ID', how='left')
    # print(df.head())
    count_no_vargroups = df[df['TMP_DEV_ID'] == np.nan].shape[0]
    if count_no_vargroups > 0:
        pass
        # print('There are devices having no vargroup')
    df.drop('TMP_DEV_ID', axis=1, inplace=True)
    del tmp_df
    return df


def compose_site_query_statement(site_id, starttime, endtime):
    # 拼接表名
    table = 'AIR_QUALITY_MEASURE_' + starttime[:4] + starttime[5:7]
    table_confidential = 'AIR_QUALITY_MEASURE_CONFIDENTIAL_' + starttime[:4] + starttime[5:7]
    print(table, '\n', table_confidential)
    site_id_list = utility.list_to_tuple(pd.unique(site_id))
    sql_statement = """
            SELECT
                SITE_ID,
                TIMESTAMP,
                AVG(CASE TYPE WHEN "1" THEN VALUE ELSE NULL END ) AS "SITE_PM25",
                AVG(CASE TYPE WHEN "2" THEN VALUE ELSE NULL END ) AS "SITE_PM10",
                AVG(CASE TYPE WHEN "3" THEN VALUE ELSE NULL END ) AS "SITE_SO2",
                AVG(CASE TYPE WHEN "4" THEN VALUE ELSE NULL END ) AS "SITE_CO",
                AVG(CASE TYPE WHEN "5" THEN VALUE ELSE NULL END ) AS "SITE_NO2",
                AVG(CASE TYPE WHEN "6" THEN VALUE ELSE NULL END ) AS "SITE_O3",
                AVG(CASE TYPE WHEN "10" THEN VALUE ELSE NULL END ) AS "SITE_TSP"
            FROM
                {0}
            WHERE
                site_id in {1}
            AND type IN (1,2,3,4,5,6,10)
            AND VALUE >0
            AND TIMESTAMP >= '{2}'
            AND TIMESTAMP <= '{3}'
            GROUP BY TIMESTAMP,site_id

            UNION ALL

            SELECT
                SITE_ID,
                TIMESTAMP,
                AVG(CASE TYPE WHEN "1" THEN VALUE ELSE NULL END ) AS "SITE_PM25",
                AVG(CASE TYPE WHEN "2" THEN VALUE ELSE NULL END ) AS "SITE_PM10",
                AVG(CASE TYPE WHEN "3" THEN VALUE ELSE NULL END ) AS "SITE_SO2",
                AVG(CASE TYPE WHEN "4" THEN VALUE ELSE NULL END ) AS "SITE_CO",
                AVG(CASE TYPE WHEN "5" THEN VALUE ELSE NULL END ) AS "SITE_NO2",
                AVG(CASE TYPE WHEN "6" THEN VALUE ELSE NULL END ) AS "SITE_O3",
                AVG(CASE TYPE WHEN "10" THEN VALUE ELSE NULL END ) AS "SITE_TSP"
            FROM
                {4}
            WHERE
                site_id in {5}
            AND type IN (1,2,3,4,5,6,10)
            AND VALUE >0
            AND TIMESTAMP >= '{6}'
            AND TIMESTAMP <= '{7}'
            GROUP BY TIMESTAMP,site_id
        """.format(table, site_id_list, starttime, endtime, table_confidential, site_id_list, starttime, endtime)
    #print(sql_statement)
    return sql_statement


def compose_monitoring_site_query_statement(city_id):
    city_str = ''
    if city_id:
        city_str = 'CITY_ID IN ('
        city_str += utility.int_list_to_str(city_id)
        city_str += ') '

    sql_statement = """
                            SELECT
                    SITE_ID
                FROM
                    MONITORING_SITE
                WHERE
                {0}
                AND DELETE_FLAG = 0 
                AND SITE_ID IN (
                SELECT DISTINCT RELATE_SITE_ID
                FROM
                    SENSOR_INFO S,
                    MEASURE_POINT M
                WHERE
                S.MEASURE_POINT_ID = M.ID
                AND RELATE_SITE_ID > 0)
    """.format(city_str)
    # print(sql_statement)
    return sql_statement


def compose_abbr_query_statement():
    sql_statement = '''
        SELECT ABBR_CODE,
               VAR_TYPE_ID,
               VAR_TYPE_TRANSDUCER_LIST
        FROM T_SENSOR_TRANSDUCER_ABBR'''
    return sql_statement


def compose_adj_data_query_statement(start_time, end_time, device_list=None):

    device_str = ''
    if device_list:
        device_str = 'AND DEV_ID IN ('
        device_str += utility.str_list_to_str(device_list)
        device_str += ') '

    table = 'DEVICE_ADJUST_VALUE_BYHOUR_' + start_time[:4] + start_time[5:7]
    sql_statement = '''
        SELECT
             DEV_ID,
             MEA_POINT_ID,
             ADJ_VALUE,
             VAR_TYPE_ID,
             ADJ_TIME,
             MARK,
             IS_NORMAL
        FROM
                {}
        WHERE
            ADJ_TIME >= '{}'
            AND ADJ_TIME <='{}'
            {}
    '''.format(table, start_time, end_time, device_str)
    # print(sql_statement)
    return sql_statement


def compose_delete_adjust_data_query_statement(dev_list, adj_time, var_type_id_list):
    var_type_id_tuple = utility.list_to_tuple(var_type_id_list)
    if len(dev_list) == 1:
        dev_tuple = '(\'{}\')'.format(dev_list[0])
    else:
        dev_tuple = tuple(dev_list)
    if dev_tuple and var_type_id_tuple and adj_time:
        where_clause = """ WHERE DEV_ID IN %s AND ADJ_TIME= '%s' AND VAR_TYPE_ID in %s """ % (dev_tuple, adj_time, var_type_id_tuple)
        return where_clause
    else:
        print('params error')

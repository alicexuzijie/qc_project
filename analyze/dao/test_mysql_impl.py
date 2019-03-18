# -*- coding: utf-8 -*-
import common
import mysql_impl
import numpy as np
import pandas as pd
# from utility import utility as tu


def test_active_devices():
    print('enter query active devices')
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_active_devices()
    # df.to_csv('a.csv')
    print(df.columns.values)
    print(df)
    print('finished')
    print('----------')


def test_active_devices_by_city_list():
    print('enter query active devices by city list')
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_active_devices_by_city(city_list=[1])
    print(df.columns.values)
    print(df)
    print('finished')
    print('----------')


def test_active_devices_by_device_list():
    print('enter query active devices by city list')
    device_list = ['YSRDTSPISJ00001388', 'YSRDPM250000006822']
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_active_devices_by_device_list(device_list=device_list)
    print(df)
    print(df.columns.values)
    print('finished')
    print('----------')


def test_query_field_name():
    dao = mysql_impl.DataOperationsByMysql()
    df_dev = dao.query_field_name()
    df_dev.to_csv('channels.csv')
    print(df_dev)


def test_query_capture_data_by_hour(starttime):
    print(starttime)
    dao = mysql_impl.DataOperationsByMysql()
    device_info_df = dao.query_active_devices()
    dic_data = dao.query_capture_data_by_hour(
        hour=starttime, device_info_df=device_info_df)
    dic_data[7].to_csv('capture_7.csv')
    print(dic_data)


def test_query_channels():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_channels()
    df.to_csv('channels.csv')
    print(df)


def test_query_qualitycontrol_version():
    print('enter query qualitycontrol version')
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_qualitycontrol_version()
    df.to_csv('qualitycontrol_version.csv')
    print(df)
    print('finished')
    print('----------')


def test_query_consistency_model():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_consistency_model()
    df.to_csv('tsp.csv')
    print(df)


def test_query_qc_dev_org_data_by_city():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_qc_dev_org_data_by_city([1], "2018-11-01 13:00:00")
    # for i in df.columns.values.tolist():
    #     print(df[i])
    print(df)
    df.to_csv('qc_dev_org_data_by_city.csv')
    print(df.columns.values)


def test_query_qc_dev_org_data_by_city_month():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_qc_dev_org_data_by_city_month([1], "2018-11-02 10:00:00")
    for i in df.columns.values.tolist():
        print(len(df[i]))
    print(df)
    print(df.columns.values)
    print(df[df['VARGROUP_ID'] == 'YSRDTSPISJ'])


def test_query_non_qc_dev_org_data_by_city():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_non_qc_dev_org_data_by_city([1], "2018-11-02 09:00:00")
    # for i in df.columns.values.tolist():
    #     print(df[i])
    print(df)
    print(df.columns.values)
    print(df[df['VARGROUP_ID'] == 'YSRDTSPISJ'])


def test_query_site_data_by_city():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_site_data_by_city([1], "2018-11-08 00:00:00")
    for i in df.columns.values.tolist():
        print(df[i])
    df.to_csv('tsp.csv')
    print(df.columns.values)
    print(df[df['SITE_ID'].isin([17941497, 17941504, 17941505, 17941488, 17941499, 17941501, 11, 17941490, 10, 17941495, 17941491])])


def test_query_devices_latitude_longitude():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_devices_latitude_longitude(relate_site_id=1)
    for i in df.columns.values.tolist():
        print(df[i])
    print(df.columns.values)


def test_query_site_latitude_longitude():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_site_latitude_longitude(city_id=[1], site_id=[1, 206])
    for i in df.columns.values.tolist():
        print(df[i])
    print(df.columns.values)


def test_query_site_data_by_hour():
    dao = mysql_impl.DataOperationsByMysql()
    df = dao.query_site_data_by_hour([1], "2018-09-05 23:00:00")
    for i in df.columns.values.tolist():
        print(df[i])
    print(df.columns.values)


# test_active_devices()
# test_active_devices_by_city_list()
# test_active_devices_by_device_list()
# test_query_channels()
# test_query_qualitycontrol_version()
# test_query_field_name()
# test_query_consistency_model()
# test_query_capture_data_by_hour('2018-11-02 15:00:00')
# test_query_qc_dev_org_data_by_city()
# test_query_qc_dev_org_data_by_city_month()
# test_query_non_qc_dev_org_data_by_city()
test_query_site_data_by_city()
# test_query_devices_latitude_longitude()
# test_query_site_latitude_longitude()
# test_query_site_data_by_hour()

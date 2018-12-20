# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181101
@filename:test_site_interpolate.py
@author:Yang
"""
from qc_site_interpolate.site_interpolate import SiteInterpolation
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig
from utility.neighbor_devices import NeighborDevices

def test_site_inter():
    hour = '2018-12-01 00:00:00'
    city_id = [771]
    config = QualityControlConfig()
    dao = DataOperationsByMysql(config, hour)
    device_list_info = dao.query_active_devices_by_city(city_id)
    device_list = device_list_info['DEV_ID'].unique().tolist()
    spatial_indexer = NeighborDevices(dao, device_list)
    dev_measure_point_id_dict = df_dict(device_list_info)
    inter = SiteInterpolation(dao, hour, spatial_indexer, device_list_info, dev_measure_point_id_dict)
    #加上is_for_var参数 是针对部分污染物进行插值的 is_for_var参数是必须的
    df = inter.execute_site_interpolate(city_id,hour,is_for_var=['PM25','PM10'])
    df.to_csv('test_all.csv')
    print(df.head())

def df_dict(dfs):
    df_temp = dfs[['DEV_ID', 'MEASURE_POINT_ID']].copy()
    return df_temp.set_index('DEV_ID').T.to_dict('list')

test_site_inter()
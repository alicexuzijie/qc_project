# encoding = utf-8

import sys
import unittest
import pandas as pd
import time
import datetime
import multiprocessing
import consistency_functions

import common
from agg_capture import AggregateCapture
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig
from aux_entities.vargroup_channels import VargroupChannels
from utility  import time_utility as tu


def new_aux_objects(hour):
    # 加载单例配置对象
    my_config = QualityControlConfig()

    # 生成数据操作层对象
    dao = DataOperationsByMysql(my_config, hour)

    # 获得vargroup相关数据
    channel_df = dao.query_channels()
    aq_dict = dao.query_aq_type_in_dict()

    vg_c = VargroupChannels(channel_df, aq_dict)

    # 获得模型相关数据
    models = dao.query_consistency_model()

    return my_config, dao, vg_c, models

def t_agg_capture(hour):
    my_config, dao, vg_c, models = new_aux_objects(hour)
    # dev_df = dao.query_active_devices()
    dev_df = dao.query_active_devices_by_city([771])
    dfs = dao.query_capture_data_by_hour(hour, dev_df)
    ac = AggregateCapture(my_config, dao, dfs, vg_c, models)
    org_dict = ac.capture_to_org(hour)
    for key in org_dict.keys():
        print(key,'\n',org_dict[key].head())
        org_dict[key].to_csv('org_{}.csv'.format(key))


def main():
    starttime = '2018-12-13 15:00:00'
    pool = multiprocessing.Pool(processes=12)
    result = []
    for i in range(24*7):
        hour = tu.datetime_n_hours_before_string(tu.time_str_to_datetime(starttime), i)
        print(hour)
        result = pool.apply_async(t_agg_capture, (hour,))
    pool.close()
    pool.join()
    if result.successful():
        print('successful')


if __name__ == "__main__":
    main()

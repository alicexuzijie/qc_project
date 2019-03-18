# encoding = utf-8

import sys
import unittest
import pandas as pd
import time
import datetime

import consistency_functions

import common
from agg_capture import AggregateCapture
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig
from aux_entities.vargroup_channels import VargroupChannels


def new_aux_objects(is_config=True, is_dao=True, is_channel=True, is_model=True):
    # 加载单例配置对象
    if is_config == True:
        my_config = QualityControlConfig()
    else:
        my_config = None

    # 生成数据操作层对象
    if is_dao == True:
        dao = DataOperationsByMysql()
    else:
        dao = None

    # 获得vargroup相关数据
    if is_channel == True:
        channel_df = dao.query_channels()
        aq_dict = dao.query_aq_type_in_dict()
        vg_c = VargroupChannels(channel_df, aq_dict)
    else:
        vg_c = None

    # 获得模型相关数据
    if is_model == True:
        models = dao.query_consistency_model()
    else:
        models = None

    return my_config, dao, vg_c, models

def test_agg_capture():
    my_config, dao, vg_c, models = new_aux_objects()
    #
    # dev_df = dao.query_active_devices_by_device_list(['YSRDPM250000002874','YSRDPM250000008068','YSRDAQ07P500001934','YSRDPM10P500000003'])
    # dev_df = dao.query_active_devices_by_device_list(['YSRDPM10P500000003'])
    # dev_df = dao.query_active_devices_by_device_list(['YSRDPM10P500000003'])
    dev_df = dao.query_active_devices()
    # dev_df = dao.query_active_devices_by_city([1])

    print('\n')

    # 按照设备清单获得相关的capture dataframe
    dfs = dao.query_capture_data_by_hour('2018-09-26 20:00:00', dev_df)
    print(dfs.keys())
    for key in dfs.keys():
        print(len(dfs[key]))

    now_time = datetime.datetime.now()
    print(now_time)
    #实例化
    ac = AggregateCapture(my_config, dao, dfs, vg_c, models)
    org_dict=ac.capture_to_org()
    for key in org_dict.keys():
        print(len(org_dict[key]))
    endtime = datetime.datetime.now()
    print(endtime-now_time)

test_agg_capture()

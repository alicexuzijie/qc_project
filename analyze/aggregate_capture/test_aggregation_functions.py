# encoding = utf-8

import sys
import unittest
import pandas as pd

import consistency_functions

import common
import consistency_functions as cons_f
import aggregation_functions as agg_f
from check_functions import clean_capture_data


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
    #初始化一个字典 存放 生成的org数据
    org = {}
    # 获得设备清单及关联信息
    # dev_df = dao.query_active_devices_by_device_list(['YSRDAQ07P500001934'])
    dev_df = dao.query_active_devices_by_device_list(['YSRDPM250000002874'])
    print(dev_df.head())
    print('\n')

    # 按照设备清单获得相关的capture dataframe
    dfs = dao.query_capture_data_by_hour('2018-09-26 20:00:00', dev_df)

    print(dfs.keys())
    print(dfs)

    full_df=pd.DataFrame()
    for var in  ['PM25', 'TEMPERATURE', 'HUMIDITY']:
        #湿度温度已添加通道
        # channels = vg_c.get_channels_by_vargroup_and_var('YSRDAQ07P5', var)
        channels = vg_c.get_channels_by_vargroup_and_var('YSRDPM2500', var)
        print(channels)

        print("befor clean")
        print(dfs[3].head())

        #判断该通道是否有效

        var_df,_,_ = clean_capture_data(dfs[3], var, channels, my_config)

        print('after clean')

        print('before applying consistency model')
        # 对原始数据应用一致性模型
        if len(var_df.columns) == 2:
            # $$$这里今后要纳入info_log，并且应该continue
            print('清洗后无有效数据')
            continue
        else:
            var_df = cons_f.apply_consistency_model(var_df, var, channels, models, my_config)

        print('afeter applying consistency model')
        print(var_df.head())

        var_df = agg_f.combine_capture_channels(var_df, var, channels, my_config)
        print('after combining channels\n')
        print(var_df)

        var_df = agg_f.agg_minutes_to_hours(var_df, var, my_config)
        print('after aggregating to hours')
        print(var_df)
        # 合并各个污染物数据
        full_df = agg_f.merge_df(var_df, full_df)
    full_df.to_csv('my_csv.csv', mode='a', header=False)
    org[3] = full_df
    print(org)
test_agg_capture()


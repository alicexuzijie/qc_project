# encoding = utf-8

import sys

from agg_capture import AggregateCapture

from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig
from aux_entities.vargroup_channels import VargroupChannels


def test_agg_capture():
    # 加载单例配置对象
    my_config = QualityControlConfig()

    # 生成数据操作层对象 
    dao = DataOperationsByMysql()

    # 获得设备清单及关联信息
    dev_df = dao.query_active_devices_by_city([1])
    print(dev_df.head())
    print('\n')

    # 按照设备清单获得相关的capture dataframe
    dfs = dao.query_capture_data_by_hour('2018-09-10 00:00:00', dev_df)
    print(dfs.keys())

    # 获得vargroup相关数据
    channel_df = dao.query_channels()
    aq_dict = dao.query_aq_type_in_dict()
    vg_c = VargroupChannels(channel_df, aq_dict)

    # 获得模型相关数据
    models = dao.query_consistency_model()

    ac = AggregateCapture(my_config, dfs, vg_c, models)
    ac.capture_to_org()

test_agg_capture()


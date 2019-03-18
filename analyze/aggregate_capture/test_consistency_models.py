# encoding = utf-8

import sys
import unittest

import consistency_functions

import common
import consistency_functions as cons_f

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
    # 获得设备清单及关联信息
    dev_df = dao.query_active_devices_by_device_list(['YSRDPM250000003811', 'YSRDPM250000008341'])
    print(dev_df.head())
    print('\n')

    # 按照设备清单获得相关的capture dataframe
    dfs = dao.query_capture_data_by_hour('2018-09-10 00:00:00', dev_df)
    print(dfs.keys())

    var = 'PM25'

    cons_f.apply_consistency_model(dfs[3], var, vg_c.get_channels_by_vargroup_and_var('YSRDPM2500', var), models, my_config)

class TestConsistencyModels(unittest.TestCase):
    def test_check_corr_validity(self):
        my_config, dao, vg_c, models = new_aux_objects(is_channel=False)

        a_valid = cons_f.check_corr_validity('PM25', 1.18, my_config)
        self.assertEqual(a_valid, False)

        a_valid = cons_f.check_corr_validity('PM25', 0.8, my_config)
        self.assertEqual(a_valid, False)

        a_valid = cons_f.check_corr_validity('PM25', 1.0, my_config)
        self.assertEqual(a_valid, True)        

        b_valid = cons_f.check_intercept_validity('PM25', 22, my_config)
        self.assertEqual(b_valid, False)

        b_valid = cons_f.check_intercept_validity('PM25', -22, my_config)
        self.assertEqual(b_valid, False)

        b_valid = cons_f.check_intercept_validity('PM25', 10, my_config)
        self.assertEqual(b_valid, True)

# unittest.main()

test_agg_capture()


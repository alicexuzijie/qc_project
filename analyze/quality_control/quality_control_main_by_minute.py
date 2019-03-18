# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181212
@filename:quality_control_main_by_minute.py.py
@author:Yang
"""
import os
import pandas as pd
import time
from config.qc_config import QualityControlConfig
from dao.mysql_impl import DataOperationsByMysql
from quality_control.quality_control_main import QualityControlRoutine
from aggregate_capture.agg_capture import AggregateCapture
from aux_entities.vargroup_channels import VargroupChannels
from utility import file_utility as fu
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class QualityControlRoutineByMinute():
    """
    类似于山西顺义这种城市，对分钟级别的数据进行质控，只做传递不做插值
    """
    def __init__(self,hour_minute):
        """
        先初始化一些接口、参数
        """
        self.config = QualityControlConfig()
        self.dao = DataOperationsByMysql(self.config, hour_minute)
        self.variables = self.config.get_config_global_data('full_pollutants')
        self.qc_routine = QualityControlRoutine(self.dao)
        self.dir = self.config.get_config_global_data('save_path_for_by_minute_qc')  #根据实际情况更改
        # self.dir = '../data/qc_data_minute'  #根据实际情况更改
        # self.adjust_df = pd.DataFrame()
        # self.interpolate_df = pd.DataFrame()
        # self.var_type_id_to_var_dict = {1:'PM25', 2:'PM10', 3:'SO2', 4:'CO', 5:'NO2', 6:'O3', 10:'TSP'}

    def init_agg_by_city_and_by_hour_minute(self, hour_minute, city_id):
        # 获得vargroup相关数据
        channel_df = self.dao.query_channels()
        aq_dict = self.dao.query_aq_type_in_dict()
        vg_c = VargroupChannels(channel_df, aq_dict)

        # 获得模型相关数据
        models = self.dao.query_consistency_model()
        #获取设备的相关信息
        self.dev_df = self.dao.query_active_devices_by_city(city_id)
        #接口要换成取前17.5分钟的capture数据
        dfs = self.dao.query_capture_data_by_minute(hour_minute, self.dev_df)
        self.ac = AggregateCapture(self.config, self.dao, dfs, vg_c, models)

    def execute_quality_control_by_minute(self, hour_minute, city_id):
        """
        按分钟质控的函数接口
        :param hour_minute:
        :param city_id:
        :return:
        """
        print('begin by minute......')

        #分钟级别的 capture 到 org计算
        self.init_agg_by_city_and_by_hour_minute(hour_minute, city_id)
        # self.qc_routine.init_qc_data_min_max()
        org_dict = self.ac.capture_to_org(hour_minute, is_for_minute=True)
        #把内存里的org数据格式转换成取非质控设备org数据那种格式
        # for key in org_dict:
        #     org_dict[key].to_csv('org_{}.csv'.format(key))
        org_df = self.prepare_org_df(org_dict)
        self.qc_routine.qc_variables = self.qc_routine.variables

        # 初始化self.spatial_indexer类
        self.qc_routine.init_spatial_indexer(city_id)
        # 初始化质控后数据不同参数的最大值和最小值
        self.qc_routine.init_qc_data_min_max()
        #先准备好数据库里已经有的质控数据  需要处理的：把var类型和var_type_id对应起来
        self.qc_routine.execute_transmission_by_city(hour_minute, city_id, is_for_minute=True, org_df=org_df)

        #存储到相关路径下
        if self.qc_routine.all_adjust_df[1].empty:
            logger.warning('该分钟级别的质控，没有出数！')
            return

        self.qc_routine.all_adjust_df[1] = self.qc_routine.set_min_and_max(self.qc_routine.all_adjust_df[1])
        save_path = fu.get_save_path(hour_minute)
        save_path = '{}/{}'.format(self.dir, save_path)
        save_name = fu.get_csv_name(hour_minute)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        self.qc_routine.all_adjust_df[1].to_csv('{}/{}'.format(save_path, save_name), index=False)

    def prepare_org_df(self, org_dict):
        """
        把内存的org数据改成取非质控设备org的格式,并且只准备非质控设备的数据
        :param org_dict:
        :return:
        """
        org_df = pd.DataFrame()
        for key in org_dict.keys():
            org_df = pd.concat([org_dict[key],org_df], axis=0)
        org_df.rename(columns = {'CAL_TIME':'TIMESTAMP'}, inplace=True)
        org_df.reset_index(drop=True, inplace=True)
        #增加vargroup_id
        org_df = org_df.merge(self.dev_df, on=['DEV_ID'], how='left')
        #取非质控设备
        non_qc_df = org_df[org_df['RELATE_SITE_ID']==-1].copy()
        #模拟字段
        need_columns = ['DEV_ID', 'PM25', 'PM10', 'SO2', 'CO', 'NO2', 'O3', 'TVOC', 'TSP',
       'HUMIDITY', 'TEMPERATURE', 'TIMESTAMP', 'SITE_ID', 'COUNT_PM25',
       'COUNT_PM10', 'COUNT_SO2', 'COUNT_CO', 'COUNT_NO2', 'COUNT_O3',
       'COUNT_TVOC', 'COUNT_TSP', 'VARGROUP_ID']
        example = pd.DataFrame(columns=need_columns)
        non_qc_df = pd.concat([example, non_qc_df], axis=0)
        non_qc_df = non_qc_df[need_columns].copy()
        return non_qc_df

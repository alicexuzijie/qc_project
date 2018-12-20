# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181126
@filename:back_calculation_main.py
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
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class BackCalculation():
    """
    需要回算的场景
    场景1：capture没有数据导致回算  capture_to_org_to_adjust 部分缺数（一小片）
            按设备清单回算非质控设备
    场景2：整个城市进行回算  capture——org 按城市计算  org--adjust  按城市（初始化）
            按城市回算（质控与非质控设备）
    """
    def __init__(self,hour):
        """
        先初始化一些接口、参数
        """
        self.config = QualityControlConfig()
        self.dao = DataOperationsByMysql(self.config, hour)
        self.variables = self.config.get_config_global_data('full_pollutants')
        self.qc_routine = QualityControlRoutine(self.dao)
        self.adjust_df = pd.DataFrame()
        self.interpolate_df = pd.DataFrame()
        self.var_type_id_to_var_dict = {1:'PM25', 2:'PM10', 3:'SO2', 4:'CO', 5:'NO2', 6:'O3', 10:'TSP', 8:'TVOC'}

    def init_agg_by_city_or_by_device_list(self,hour, city_id, device_list=None):
        # 获得vargroup相关数据
        channel_df = self.dao.query_channels()
        aq_dict = self.dao.query_aq_type_in_dict()
        vg_c = VargroupChannels(channel_df, aq_dict)

        # 获得模型相关数据
        models = self.dao.query_consistency_model()
        #获取设备的相关信息
        if device_list is not None:
            dev_df = self.dao.query_active_devices_by_device_list(device_list)
        else:
            dev_df = self.dao.query_active_devices_by_city(city_id)
        dfs = self.dao.query_capture_data_by_hour(hour, dev_df)
        self.ac = AggregateCapture(self.config, self.dao, dfs, vg_c, models)

    def execute_back_calculation(self, hour, city_id, is_for_org=False, var_names=None, dev_list=None):
        """
        回算的主函数
        :param hour: 回算的时间
        :param dev_list: 设备是 list类型
        :param city_id: 城市id 输入的类型必须是 list
        :return: 直接入库
        """
        print('begin back cal......')
        if dev_list is not None:
            # 按设备清单回算非质控设备
            #capture 到 org回算
            self.init_agg_by_city_or_by_device_list(hour, city_id, device_list=dev_list)
            org_dict = self.ac.capture_to_org(hour)
            # for key in org_dict.keys():
            #     org_dict[key].to_csv('org_{}.csv'.format(key))
            # org 到 adjust
            if var_names:
                self.qc_routine.variables = var_names
            self.qc_routine.qc_variables = self.qc_routine.variables

            # 初始化self.spatial_indexer类
            self.qc_routine.init_spatial_indexer(city_id)
            # 初始化质控后数据不同参数的最大值和最小值
            self.qc_routine.init_qc_data_min_max()
            #先准备好数据库里已经有的质控数据  需要处理的：把var类型和var_type_id对应起来
            adjust_df_from_db = self.prepare_adjust_df(hour)
            self.qc_routine.execute_transmission_by_city(hour, city_id, dev_list=dev_list)
            adjust_df_all = pd.concat([self.qc_routine.all_adjust_df[1].copy(), adjust_df_from_db.copy()], axis=0)
            # self.qc_routine.all_adjust_df[1].copy().to_csv('A.csv')
            # adjust_df_from_db.to_csv('B.csv')
            # adjust_df_all.to_csv('C.csv')
            #去重
            adjust_df_all = adjust_df_all.groupby(['DEV_ID', 'VAR']).first()
            adjust_df_all.reset_index(inplace=True)
            # adjust_df_all = adjust_df_all.groupby(['DEV_ID', 'VAR']).first()
            # adjust_df_all.reset_index(inplace=True)
            self.qc_routine.adjust_df_full = adjust_df_all

            #对已经有的adjust进行审核
            # 初始化审核函数
            self.qc_routine.init_check(city_id, hour)
            self.qc_routine.execute_adj_data_censor(dev_list=dev_list)

            #进行插值
            # #初始化插值类 计算插值后的数据
            self.qc_routine.execute_interpolate_by_city(hour, city_id, dev_list=dev_list)

            for key in self.qc_routine.all_adjust_df.keys():
                if not self.qc_routine.all_adjust_df[key].empty:
                    self.dao.write_adjust_data(self.qc_routine.all_adjust_df[key], hour)
                    # self.qc_routine.all_adjust_df[key].to_csv('adjust_{}.csv'.format(key))
                else:
                    continue

        else:
            if is_for_org:
                # 按城市进行回算
                #实例化 AggregateCapture 类 对某个城市进行capture到org的计算
                self.init_agg_by_city_or_by_device_list(hour, city_id=city_id)
                self.ac.capture_to_org(hour)
                #对某个城市的设备进行质控
                if var_names:
                    self.qc_routine.variables = var_names
                self.qc_routine.obtain_adjust_data(city_id,hour)
                return
            else:
                #省略capture到org的过程
                #对某个城市的设备进行质控
                if var_names:
                    self.qc_routine.variables = var_names
                self.qc_routine.obtain_adjust_data(city_id,hour)
                return

    def prepare_adjust_df(self, hour):
        adjust_df = self.dao.query_adj_data_by_device_list(self.qc_routine.device_list, hour, hour)
        adjust_df['VAR'] = adjust_df.apply(lambda x:self.var_type_id_to_var_dict[x.VAR_TYPE_ID], axis=1)
        return adjust_df

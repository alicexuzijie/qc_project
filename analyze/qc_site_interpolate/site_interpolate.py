# encoding=utf-8

import common
from utility.distance_utility import weighted_mean_by_distance
from utility.neighbor_devices import NeighborDevices
from utility import time_utility as tu
from config.qc_config import QualityControlConfig
from aux_entities.vargroup_channels import VargroupChannels
import datetime
import pandas as pd
import numpy as np
import random

from log import log
from error_demo.error_code import *
logger = log.log_demo()


class SiteInterpolation():

    def __init__(self, dao, hour, spatial_indexer, dfs, dev_measure_point_id_dict):
        '''
        这个类是在最极端的情况下对设备进行插值，将设备附近子站的监测值赋给自己.
        此模块相对比较独立所以初始化要完整
        1. 插值时所需要的最近点个数，用于按照距离平方的倒数取反
        '''
        self.hour = hour
        # 该城市在线的设备信息
        self.dfs = dfs
        #dfs 中的点位信息转化成字典形式
        self.dev_measure_point_id_dict = dev_measure_point_id_dict
        self.config = QualityControlConfig()
        self.dao = dao
        channel_df = self.dao.query_channels()
        aq_dict = self.dao.query_aq_type_in_dict()
        self.vargroup_channels = VargroupChannels(channel_df, aq_dict)
        self.vars = self.config.get_config_global_data('full_pollutants')
        # self.N = 20  #指定查找设备最近N公里范围内的子站
        self.tsp_over_pm10_min = self.config.get_config_var_data('tsp_over_pm10_min','TSP')
        self.tsp_over_pm10_max = self.config.get_config_var_data('tsp_over_pm10_max','TSP')
        self.inter_site_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])
        self.spatial_indexer = spatial_indexer

        self.N = {}
        for var in self.vars:
            self.N[var] = self.config.get_config_var_data('num_nearest_sites_for_interpolate', var)

    # def init_neighbor(self,city):
    #     self.spatial_indexer = NeighborDevices(self.dao, city_id=city)

    def execute_site_interpolate(self, city, hour, is_for_var, site_df, mark, dev_list=None):
        """
        执行子站插值
        :param city:
        :param hour:
        :param is_for_var:
        :param site_df: 当小时的子站数据，从外面传进来是经过审核的
        :param mark: 处理的质控数据的类型
        :return:
        """
        print("进入子站插值模式，vars: {}".format(is_for_var))
        #1、获取当小时的子站数据
        site_df = self.prepare_site_data(site_df)
        #取一下当小时数据，因为这里是三个小时的数据
        site_df = site_df[site_df['TIMESTAMP'] == hour].copy()
        # logger.info('子站数据：{}'.format(len(site_df)))

        if dev_list is not None:
            #进入设备编号的插值阶段
            dev_vargroup_list = dev_list

            cur_vargroup_vars_to_interpolate = is_for_var
            self.get_site_inter_df(cur_vargroup_vars_to_interpolate, dev_vargroup_list, site_df, mark, hour)

        else:
            #2进行插值计算  整个城市的is_for_var列表
            dev_list = self.dfs['DEV_ID'].unique().tolist()

            logger.info('城市{}在线的设备一共{}'.format(city, len(dev_list)))
            vargroup_list = self.dfs['VARGROUP_ID'].unique().tolist()
            for vargroup in vargroup_list:
                df = self.dfs[self.dfs['VARGROUP_ID']==vargroup]
                dev_vargroup_list = df['DEV_ID'].unique().tolist()
                var_names = self.vargroup_channels.get_var_names_by_vargroup(vargroup)
                # print('{} : {}'.format(vargroup, var_names))
                var_names = self.drop_var_names(var_names)
                cur_vargroup_vars_to_interpolate = list(set(var_names).intersection(set(is_for_var)))
                logger.warning('s^s^s^ 城市 {} Vargroup {} 需要被插值的参数: {}'.format(city, hour, cur_vargroup_vars_to_interpolate))
                if len(cur_vargroup_vars_to_interpolate) > 0:
                    # single_var_names = is_for_var
                    self.get_site_inter_df(cur_vargroup_vars_to_interpolate, dev_vargroup_list, site_df, mark, hour)
                else:
                    # print('vargroup{}不需要插值'.format(vargroup))
                    continue

        return self.inter_site_df.copy()

    def clear_inter_site_df(self):
        self.inter_site_df.drop(self.inter_site_df.index, inplace=True)
        print(self.inter_site_df)

    def get_site_inter_df(self, var_names, dev_vargroup_list, site_df, mark, hour):
        for p in var_names:
            # 如果需要插值的设备数量为0，直接return
            if len(dev_vargroup_list) == 0:
                return
            for dev_id in dev_vargroup_list:
                site_inter_val = self.interpolation_for_dev(dev_id, p, site_df)
                if (site_inter_val == -1) | (site_inter_val == None):
                    logger.warning('s^s^s^{}设备在{}参数下没有合理的子站补充插值数据{}'.format(dev_id, p, site_inter_val))
                    continue
                else:
                    site_inter_val = site_inter_val + random.uniform(0, 10)
                    mea_point_id = self.dev_measure_point_id_dict[dev_id][0]
                    inter_dict = {'DEV_ID': dev_id, 'MEA_POINT_ID': mea_point_id, 'ADJ_VALUE': site_inter_val,
                                  'VAR': p, 'VAR_TYPE_ID': None, 'ADJ_TIME': hour, 'MARK': mark, 'IS_NORMAL': 1}

                    self.inter_site_df = self.inter_site_df.append(inter_dict, ignore_index=True)

    def interpolation_for_dev(self, dev_id, var_name, site_df):
        """
        1、先把某城市的子站数据全部加载到内存中
        2. 给定任意的设备编号，查找其附近的子站，拿到距离
        3. 获取临近子站的数据，并按照距离的平方倒数为权重进行加权平均
        :param dev_id: 需要插值的设备编号
        :param var_names: 需要插值的污染物
        :param site_df: 就是子站的数
        :return:
        """
        #1. 给定任意的设备编号，查找其附近的子站，拿到距离
        nearest_sites_dict = self.spatial_indexer.find_nearest_site_by_num(dev_id, self.N[var_name])
        site_df_index = site_df.set_index(['SITE_ID'])

        if nearest_sites_dict is not None:
            nearest_sites = list(nearest_sites_dict.keys())
            distance_list = []
            value_list = []
            # 2. 获取邻近子站的测量数据，并按照距离的平方倒数为权重进行加权平均
            site_value_df = site_df[site_df['SITE_ID'].isin(nearest_sites)]
            if site_value_df.empty:
                logger.warning("var：{} 设备s^s^s^{}周围子站都没有数据，因此不能出插值！周围子站编号:{}".format(var_name, dev_id, nearest_sites))
            else:
                value_nearest_sites = site_value_df['SITE_ID'].unique()
                for site in value_nearest_sites:
                    if not np.isnan(site_df[site_df['SITE_ID']==site]['SITE_'+var_name].values):
                        # 距离和值是一一对应的关系
                        if nearest_sites_dict[site] == 0:
                            #距离为0时设置为1
                            distance = nearest_sites_dict[site] + 1
                        else:
                            distance = nearest_sites_dict[site]
                        distance_list.append(distance)
                        # value_list.append(site_df[site_df['SITE_ID']==site]['SITE_'+var_name].values[0])
                        value_list.append(site_df_index.loc[(site), 'SITE_'+var_name])
                    else:
                        continue
            try:
                interpolate_val = weighted_mean_by_distance(2, distance_list, value_list)
                return interpolate_val
            except BaseError as e:
                e.setdata({'报错位置':'子站插值', 'dev_id':dev_id, 'var':var_name})
                logger.info('因为找不到邻近子站，无法为设备{}的参数{}子站插值'.format(dev_id, var_name))
                return None
        else:
            logger.info('该设s^s^s^{}附近没有子站！'.format(dev_id))
            return None

    def prepare_site_data(self, site_df):
        """
        获取某个城市当小时下的子站数据, 并且针对没有TSP的子站进行虚拟，将PM10的值乘上一个系数 虚拟出TSP的值
        :param city_id:
        :param hour:
        :return:
        """
        # start_hour = tu.str_datetime_n_hours_before_string(hour, 2)
        # site_df = self.dao.query_site_data_by_time_interval(start_hour= start_hour, end_hour=hour,city_id=city_id)
        # print("虚拟子站数据".format(site_df.info()))
        # print('子站数据'.format(site_df.head()))
        site_df['SITE_TSP'] = site_df.apply(
            lambda x: x.SITE_PM10 * random.uniform(self.tsp_over_pm10_min, self.tsp_over_pm10_max) if np.isnan(
                x.SITE_TSP) else x.SITE_TSP, axis=1)
        return site_df

    def drop_var_names(self, var_names):
        no_need_var_names = ['TEMPERATURE', 'HUMIDITY', 'TVOC']
        for no_need_var in no_need_var_names:
            if no_need_var in var_names:
                var_names.remove(no_need_var)
        return var_names

    def df_dict(self,dfs):
        df_temp = dfs[['DEV_ID','MEASURE_POINT_ID']].copy()
        return df_temp.set_index('DEV_ID').T.to_dict('list')

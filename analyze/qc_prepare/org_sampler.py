import numpy as np
import random
import pandas as pd

import common

from error_demo.error_code import *
from log import log
import utility.time_utility as tu
logger = log.log_demo()


class OrgSampler():
    '''
    本类主要针对不用区分设备的情况下进行通用的无效org数据筛除
    '''
    def __init__(self, config):
        '''
        将本类中用到的config值放到内存中，避免经过config类的反复校验，加快运行速度
        '''
        self.config = config
        self.oversampling_count = {}
        self.prev_hour_oversampling_count = {}
        self.undersampling_prob = {}
        self.high_low_humidity_cutting = {}

        self.pollutants = config.get_config_global_data('full_pollutants')
        self.variables_sensitive_to_high_humidity = config.get_config_global_data("vars_sensitive_to_high_hum")
        self.variables_to_oversample = config.get_config_global_data("vars_to_oversample")
        self.init_key_sampler_parameters()

    def init_key_sampler_parameters(self):
        '''
        初始化关键的采样器参数，包括对当小时oversampling的次数，在当小时湿度较小时，对前面湿度较大的时期undersampling时的采样概率以及湿度大小判定的分界线
        '''
        self.init_oversampling_parameters()
        self.init_undersampling_parameters()

    def init_oversampling_parameters(self):
        '''
        初始化oversampling相关参数
        '''
        for p in self.pollutants:
            try:
                oversample_count = self.config.get_config_var_data("oversampling_count", p)
                self.oversampling_count[p] = oversample_count
            except ParameterRangeError as e:
                e.setdata({'查询参数':p, '查询项':'oversample_count'})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(), exc_info=True)
            try:
                prev_oversample_count = self.config.get_config_var_data("prev_hour_oversampling_count", p)
                self.prev_hour_oversampling_count[p] = prev_oversample_count
            except ParameterRangeError as e:
                e.setdata({'查询参数':p, '查询项':'oversample_count'})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(), exc_info=True)

    def init_undersampling_parameters(self):
        '''
        初始化undersampling相关参数
        '''
        for p in self.pollutants:
            try:
                undersampling_prob = self.config.get_config_var_data("undersampling_prob", p)
                self.undersampling_prob[p] = undersampling_prob
            except ParameterRangeError as e:
                e.setdata({'查询参数':p, '查询项':'undersampling_prob'})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(), exc_info=True)

            try:
                high_low_humidity_cutting = self.config.get_config_var_data("high_low_humidity_cutting", p)
                self.high_low_humidity_cutting[p] = high_low_humidity_cutting
            except ParameterRangeError as e:
                e.setdata({'查询参数':p, '查询项':'high_low_humidity_cutting'})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(), exc_info=True)

    def cur_hour_oversampler(self, df, var, target_hour):
        '''
        对当小时进行oversampling

        Args:
            df: 需要处理的dataframe
            var: 当前dataframe涉及的参数, e.g., PM25
            target_hour: 需要做质控的目标整点
        '''
        if var in self.variables_to_oversample:
            cur_hour_df = df[df['TIMESTAMP'] == target_hour].copy()

            # 如果当小时的数据存在，则perform oversampling
            if cur_hour_df.shape[0] > 0:
                df = df.append([cur_hour_df] * self.oversampling_count[var])
                df.reset_index(inplace=True, drop=True)

            else:
                prev_hour = tu.str_datetime_to_int_hour_minus_one_hour(target_hour)
                prev_hour = tu.datetime_to_string(prev_hour)
                # 如果当小时的数据不存在，则对上个小时进行oversampling
                prev_hour_df = df[df['TIMESTAMP'] == prev_hour].copy()

                if prev_hour_df.shape[0] > 0:
                    df = df.append([prev_hour_df] * self.prev_hour_oversampling_count[var])
                    df.reset_index(inplace=True, drop=True)

        return df

    def high_humidity_undersampler(self, df, var, target_hour):
        '''
        如果当小时湿度小于给定阈值，而训练数据中出现了较大湿度的数据，则对较大湿度的数据按undersampling采样的概率进行排除

        Args:
            df: 需要处理的dataframe
            var: 当前dataframe涉及的参数, e.g., PM25
            target_hour: 需要做质控的目标整点
        '''
        if var in self.variables_sensitive_to_high_humidity:
            df = self.undersample_for_high_humidity(df, var, target_hour)
        return df

    def undersample_for_high_humidity(self, df, var, target_hour):
        '''
        对高湿度的数据进行undersampling

        Args:
            df: 需要处理的dataframe
            var: 需要处理的参数
        '''
        device_list = df['DEV_ID'].unique()
        # print(device_list)
        prob = self.undersampling_prob[var]
        cutting_point = self.high_low_humidity_cutting[var]
        undersample_df = pd.DataFrame()

        cur_dev_hour_humidity = np.nan

        for dev in device_list:
            tmp_df = df[df['DEV_ID'] == dev].copy()
            tmp_cur_hour_df = tmp_df[tmp_df['TIMESTAMP'] == target_hour]
            if tmp_cur_hour_df.shape[0] > 0:
                cur_dev_hour_humidity = np.mean(tmp_cur_hour_df['HUMIDITY'])
            else:
                prev_hour = tu.str_datetime_to_int_hour_minus_one_hour(target_hour)
                target_hour = tu.datetime_to_string(prev_hour)
                tmp_prev_hour_df = tmp_df[tmp_df['TIMESTAMP'] == target_hour]
                if tmp_prev_hour_df.shape[0] > 0:
                    cur_dev_hour_humidity = np.mean(tmp_prev_hour_df['HUMIDITY'])

            if (cur_dev_hour_humidity != np.nan) & (cur_dev_hour_humidity < cutting_point):
                tmp_df['IS_EXCLUDE'] = tmp_df.apply(lambda x: self.is_exclude_for_high_hum(x.HUMIDITY, prob, cutting_point), axis=1)

                tmp_df = tmp_df[(tmp_df['IS_EXCLUDE'] == -1) | (tmp_df['TIMESTAMP'] == target_hour)].copy()
                # tmp_df = tmp_df.reset_index()
                tmp_df.drop(columns=['IS_EXCLUDE'], inplace=True)

            undersample_df = pd.concat([undersample_df, tmp_df], axis=0, sort=False)

        undersample_df.reset_index(drop=True, inplace=True)

        return undersample_df

    def is_exclude_for_high_hum(self, humidity, prob, cutting_point):
        '''
        如果需要保留该行数据则返回-1，否则返回1
        '''
        if humidity > cutting_point:
            rnd = random.random()
            if rnd < prob:
                return -1
            else:
                return 1
        else:
            return -1
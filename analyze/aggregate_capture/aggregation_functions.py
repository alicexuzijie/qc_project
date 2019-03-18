# encoding = utf-8

import numpy as np
import pandas as pd

import utility.utility as utility
import utility.time_utility as tu

from log import log
from error_demo.error_code import *
logger = log.log_demo()

class AggregationAgent():
    def __init__(self, config):
        """
        初始化类
        :param config: 传入单例配置项对象
        """
        pollutants = ['PM25', 'PM10']
        self.prob_deviation_threshold = {}
        self.prob_deviation_percent_threshold = {}

        for p in pollutants:
            self.prob_deviation_threshold[p] = config.get_config_var_data('prob_deviation_threshold', p)
            self.prob_deviation_percent_threshold[p] = config.get_config_var_data('prob_deviation_percent_threshold', p)

    def combine_capture_channels(self, df, var, channels):
        """
        1. 针对多通道数据进行合并
        2. 合并后的值存为一列，命名为var
        3. drop掉原来的多通道数据
        :param df:待处理的数据
        :param var:污染物名称（列名）
        :param channels:通道
        :return:通道合并后的df数据
        """
        # logger.info('begin combine_capture_channels.....{}'.format(channels))

        if len(channels) == 1:
            # 重命名该dataframe相关的列
            df.rename(columns={channels[0]:var}, inplace=True)
            return df

        elif len(channels) == 2:
            # 直接取平均值
            df[var] = df.apply(lambda row: np.mean(row[channels]), axis=1)
            df = utility.drop_columns_for_dataframe(df, channels)
            df.dropna(inplace=True)
            return df

        elif len(channels) > 2:
            df_tmp=df[channels].copy()
            df['COUNT'] = np.isnan(df_tmp).sum(axis=1)
            df['MEDIAN'] = df.apply(lambda row: np.median(row[channels]) if row['COUNT']==0 else np.mean(row[channels]), axis=1)
            df = df[np.isnan(df['MEDIAN']) == False]

            df[var] = df.apply(lambda row:self.average_excl_deviation(var, row, channels), axis=1)
            df = utility.drop_columns_for_dataframe(df, channels + ['MEDIAN','COUNT'])
            df.dropna(inplace=True)
            return df
        else:
            # $$$未来应该被容错
            logger.debug('No channels selected!')

    def average_excl_deviation(self, var, row, channels):
        """
        排除超出deviaiton以外的通道值之后取均值
        :param var: 待处理的参数，如PM25
        :param row: 待处理的行，行里包括各通道读数及MEDIAN值
        :param channels: 各出数通道名
        :return: 排除与Median相比绝对偏差大于阈值或者相对偏差大于阈值的通道后的平均值
        """
        val = 0
        cnt = 0
        for channel in channels:
            if self.is_within_deviation_value(var, row[channel], row['MEDIAN']) or self.is_within_deviation_percent(var, row[channel], row['MEDIAN']):
                val += row[channel]
                cnt += 1
            else:
                continue
        if cnt == 0:
            logger.debug('聚合时因为两个通道差别太大导致不知道选哪个通道而放弃！设备编号{}'.format(row['DEV_ID']))
            # logger.info('something is wrong, b/c at least the median is not nan')
            return None
        else:
            return val/cnt

    def agg_minutes_to_hours(self, df, var, hour_minute=None):
        """
        将分钟级的dataframe聚合成小时平均值，并且给出其有效读数条数
        :param df:
        :param var:
        :return:
        """
        if hour_minute:
            df['CAL_TIME'] = hour_minute
        else:
            # logger.info('begin agg_minutes_to_hours.....')
            df['CAL_TIME'] = df.apply(lambda x: tu.datetime_to_int_hour(x.CAP_TIME), axis=1)

        # 获取有效数据的条数
        hour_cnt = df.groupby(['DEV_ID', 'CAL_TIME', 'MEA_POINT_ID'])[[var]].count()
        hour_cnt.rename(columns={var:'COUNT_{}'.format(var)}, inplace=True)

        # 获取小时平均值
        hour_val = df.groupby(['DEV_ID', 'CAL_TIME', 'MEA_POINT_ID'])[[var]].mean()

        hour_cnt_val = hour_cnt.merge(hour_val, left_index=True, right_index=True, how='inner')
        hour_cnt_val.reset_index(inplace=True)

        del df
        return hour_cnt_val


    def is_within_deviation_value(self, var, val, benchmark):
        """
        计算每个通道的监测值和中位数的差值绝对值，如果在阈值内就返回True，否则返回False
        :param var:
        :param val:
        :param benchmark:
        :return:
        """
        if abs(val-benchmark) <= self.prob_deviation_threshold[var]:
            return True
        else:
            return False


    def is_within_deviation_percent(self, var, val, benchmark):
        """
        计算每个通道的监测值和中位数的差值并做比值计算，返回百分比，如果在阈值内就返回True，否则返回False
        :param var:
        :param val:
        :param benchmark:
        :return:
        """
        if (abs(val-benchmark) / benchmark)  <= self.prob_deviation_percent_threshold[var]:
            return True
        else:
            return False

    def merge_df(self, df_set, full_df):
        """
        将每个污染物处理后产生的df进行合并，按列进行outer合并
        :param df_set:
        :param full_df:
        :return:
        """
        if len(full_df)==0:
            full_df = df_set
        else:
            full_df=df_set.merge(full_df, how='outer', on=['DEV_ID', 'CAL_TIME', 'MEA_POINT_ID'])
        return full_df

    def concat_df(self, df_set, org_vargroupid):
        """
        将每个vargroup下的df数据处理后进行合并，按行outer合并
        :param df_set:
        :param org_vargroupid:
        :return:
        """
        if len(org_vargroupid)==0:
            org_vargroupid = df_set
        else:
            org_vargroupid=pd.concat([df_set, org_vargroupid], join='outer', sort=False)
        return org_vargroupid

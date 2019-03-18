# encoding = utf-8

import pandas as pd
import numpy as np

from log import log
from error_demo.error_code import *
logger = log.log_demo()

pd.set_option('display.max_columns', 20)


class ConsistencyAgent():
    def __init__(self, config):
        pollutants = ['PM25', 'PM10', 'CO', 'NO2', 'SO2', 'O3', 'TSP']
        self.a_min = {}
        self.b_min = {}
        self.a_max = {}
        self.b_max = {}

        for p in pollutants:
            self.a_min[p] = config.get_config_var_data('consistency_model_a_min', p)
            self.a_max[p] = config.get_config_var_data('consistency_model_a_max', p)            
            self.b_min[p] = config.get_config_var_data('consistency_model_b_min', p)
            self.b_max[p] = config.get_config_var_data('consistency_model_b_max', p)

    def apply_consistency_model(self, df, var, var_channels, models):
        """
        对于给定的变量，取出来每一个通道，检查模型是否有效，如果模型在有效范围内，则应用模型，否则不应用
        :param df: 针对某个变量，例如PM25或者PM10的capture数据，注意其中如果该变量在这个vargroup的设备上是多的通道的话，
            那么df也包含多通道的数据
        :param var: 变量名称，用于调用config相关内容
        :param var_channels: 每个污染物对应的出数通道
        :param models: 从normalization_model表中取出来的所有模型
        :return: 应用完一致性模型后的df
        """
        # logger.info('begin apply_consistency_model....')
        # 取出设备的每一个通道
        if var in ['TEMPERATURE','HUMIDITY','TVOC']:
            return df
        else:
            cur_df = df[['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID']].copy()
            for channel in var_channels:

                # 将测量数据与模型merge成一个dataframe

                channel_model_df = self.measurement_model_merge(df, models, channel)

                if channel_model_df.empty:
                    cur_df[channel] = cur_df.apply(lambda x: np.nan, axis=1)
                    logger.debug('None of the devices have model for this channel {}'.format(channel))
                else:
                    # 检查每一个设备的对应通道模型的validity
                    channel_model_df['IS_MODEL_VALID'] = channel_model_df.apply(lambda x: self.check_consistency_model_validity(var, x.ARG1, x.ARG2), axis=1)

                    # 将无效的模型设备编号添加到日志里
                    dev_no_valide = channel_model_df[channel_model_df['IS_MODEL_VALID'] == False].groupby(['DEV_ID'])[
                        ['ARG1', 'ARG2']].first()
                    dev_no_valide.reset_index(inplace=True)
                    logger.debug('一致性模型无效的设备信息：{}'.format(dev_no_valide))

                    # 只保留valid的通道
                    channel_model_df = channel_model_df[channel_model_df['IS_MODEL_VALID'] == True]

                    # 如果所有设备都不valid，log_info
                    if channel_model_df.shape[0] > 0:

                        channel_model_df[channel] = channel_model_df.apply(lambda row: self.cal_consistency_output(row, channel), axis=1)

                        post_consist_model_df = self.trim_columns(channel_model_df)

                        cur_df = cur_df.merge(post_consist_model_df, left_on=['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID'], right_on=['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID'], how='left')

                    else:
                        cur_df[channel] = cur_df.apply(lambda x: np.nan, axis=1)
                        logger.debug('None of the devices have valid model for this channel {}'.format(channel))
            return cur_df


    def trim_columns(self, channel_model_df):
        """
        对不需要的列进行drop
        :param channel_model_df: 针对每个通道，合并了该通道的模型之后得到的data frame
        :return: drop了不需要的列之后的dataframe
        """

        channel_model_df.reset_index(drop=True, inplace=True)
        channel_model_df.drop(columns=['ARG1', 'ARG2', 'IS_MODEL_VALID', 'CHANNEL'], inplace=True)
        return channel_model_df


    def cal_consistency_output(self, row, channel):
        """
        将设备测量值代入一致性模型进行计算之后的值
        :param row: df.apply函数中的lambda row
        :param channel: 对应的出数通道，string
        :return: 经过一致性模型之后的值
        """

        return row['ARG1']*row[channel] + row['ARG2']


    def measurement_model_merge(self, data_df, model_df, channel):
        """
        将测量值与一致性模型进行merge，方便进行df.apply操作
        :param data_df: capture数据
        :param model_df: 一致性模型['DEV_ID', 'ARG1', 'ARG2']
        :param channel: 出数通道
        :return: 合并之后的dataframe
        """
        cur_df = data_df[['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID', channel]].copy()
        cur_df.dropna(inplace=True)

        l1 = cur_df.DEV_ID.unique()

        cur_channel_model = model_df[model_df['CHANNEL'] == channel].copy()
        cur_df = cur_df.merge(cur_channel_model, left_on='DEV_ID', right_on='DEV_ID')

        l2 = cur_df.DEV_ID.unique()
        dev_list = set(list(l1)) - set(list(l2))
        logger.debug('没有该通道{} 一致性模型的设备编号：{}'.format(channel, dev_list))
        del cur_channel_model
        return cur_df


    def check_consistency_model_validity(self, var, a, b):
        """
        检查一致性模型是否合格
        :param var: 对应的污染物名称，如'PM25', 'PM10'
        :param a: 一致性模型的系数（斜率）
        :param b: 一致性模型的截距
        :return: True/False
        """
        a_valid = self.check_corr_validity(var, a)
        b_valid = self.check_intercept_validity(var, b)

        if (a_valid == True) & (b_valid == True):
            return True
        else:
            # $$$ 未来需要raise error，表明为什么不通过这个通道
            #logger.info('a_valid is {},b_valid is {}'.format(a_valid,b_valid))
            return False


    def check_corr_validity(self, var, a):
        """
        判断系数是否有效
        :param var:
        :param a:
        :return: True/False
        """
        if (a >= self.a_min[var]) & (a <= self.a_max[var]):
            return True
        else:
            #logger.info('A is {}'.format(a))
            return False


    def check_intercept_validity(self,  var, b):
        """
        判断斜率是否有效
        :param var:
        :param b:
        :return: True/False
        """
        if (b >= self.b_min[var]) & (b <= self.b_max[var]):
            return True
        else:
            #logger.info('B is {}'.format(b))
            return False

import numpy as np

import common
from qc_scenario.scenarios import QCScenarios


class OrgFilter():
    '''
    本类主要针对不用区分设备的情况下进行通用的无效org数据筛除
    '''
    def __init__(self, config):
        '''
        将本类中用到的config值放到内存中，避免经过config类的反复校验，加快运行速度
        '''
        pollutants = config.get_config_global_data('full_pollutants')
        self.effective_capture_per_hour = {}
        self.qc_data_headers = {}
        self.non_qc_data_headers = {}
        self.enum_scenarios = QCScenarios
        for p in pollutants:
            self.effective_capture_per_hour[p] = config.get_config_var_data('effective_capture_per_hour', p)
            self.qc_data_headers[p] = config.get_config_var_data('qc_data_headers', p)
            self.non_qc_data_headers[p] = config.get_config_var_data('non_qc_data_headers', p)

        self.sand_storm_thres = config.get_config_global_data('sand_storm_threshold')
        self.sand_storm_dev_thres = config.get_config_global_data('sand_storm_dev_deviation_threshold')

        self.num_min_train_points = config.get_config_global_data('dev_effective_entries_threshold')

    def few_entry_filter(self, df, var):
        '''
        对每小时capture数据条数不足的数据进行过滤
        Args:
            df: 只包含必须列的数据
            var: 待处理的变量
        Return:
            满足该variable每小时capture数据条数的dataframe 
        '''
        # 首先drop掉COUNT_VAR为nan的数据
        var_df = df[np.isnan(df['COUNT_{}'.format(var)]) == False]

        if var_df.empty:
            # print('参数{}没有数据'.format(var))
            return var_df
        # 选取符合有效条数的数据
        var_df = var_df[var_df['COUNT_{}'.format(var)] >= self.effective_capture_per_hour[var]].copy()

        # $$$这里可以考虑输出日志过滤掉了多少条数据
        return var_df

    def column_filter(self, df, var, is_for_train=True):
        '''
        过滤掉训练该参数不需要的列

        Args:
            df: dao返回的org数据，包含多参数
            var: 待处理的变量
        Return:
            只包含必须列的dataframe
        '''
        list_of_must_columns = ['DEV_ID', 'TIMESTAMP']
        if is_for_train:
            list_of_useful_columns = self.qc_data_headers[var]
        else:
            list_of_useful_columns = self.non_qc_data_headers[var]
        df = df[list_of_must_columns + list_of_useful_columns]
        return df

    def nan_filter(self, df):
        '''
        过滤掉为nan的数据

        Args:
            df: 只包含必须列的数据

        Return:
            过滤掉nan的、只包含必须列的dataframe
        '''
        df.dropna(inplace=True)
        return df

    def var_specific_filter(self, df, var, scenario):
        '''
        针对每个var进行相关的特点，进行训练数据的行筛选

        '''
        if var == 'PM10':
            return self.sand_storm_filter(df, var, scenario)
        else:
            return df

    def sand_storm_filter(self, df, var, scenario):
        if var == 'PM10':
            df['SITE_PM10_OVER_PM25'] = df.apply(lambda x: x.SITE_PM10/x.SITE_PM25, axis=1)
            df['DEV_PM10_OVER_PM25'] = df.apply(lambda x: x.PM10/x.PM25, axis=1)
            if scenario == self.enum_scenarios.SAND_STORM:
                return df
            else:
                df['IS_SAND_STORM_HOUR'] = df.apply(lambda x: 1 if x.SITE_PM10_OVER_PM25 > self.sand_storm_thres and x.SITE_PM10_OVER_PM25 - x.DEV_PM10_OVER_PM25 > self.sand_storm_dev_thres else 0, axis=1)
                df = df[df['IS_SAND_STORM_HOUR'] == 0]
                return df
        else:
            return df

    def not_enough_device_entry_filter(self, df):
        '''
        针对获取数据里的每一个设备，如果其训练条数不满足最低条数要求，则过滤掉
        '''
        device_list = df['DEV_ID'].unique()
        df.set_index(['DEV_ID'], drop=True, inplace=True)

        for dev in device_list:
            # print('the dev {} size{}'.format(dev, df[df.index == dev].shape[0]))
            # print('dev_effective_entries_threshold is {}'.format(self.num_min_train_points))
            if df[df.index == dev].shape[0] >= self.num_min_train_points:
                continue
            else:
                df.drop(dev, axis=0, inplace=True)

        df.reset_index(inplace=True)

        return df

    def current_hour_no_data_device_filter(self, df, hour):
        """
        针对当小时没有数据的设备进行过滤
        :param df:已经过滤过的训练数据
        :param hour:当前时间
        :return:
        """

        dev_list = df[df['TIMESTAMP']==hour]['DEV_ID'].unique().tolist()
        print('当小时下的设备{}'.format(len(dev_list)))
        df = df[df['DEV_ID'].isin(dev_list)]
        after_dev_list = df[df['TIMESTAMP'] == hour]['DEV_ID'].unique().tolist()
        print('清洗后当小时下的设备{}'.format(len(after_dev_list)))
        df.reset_index(inplace=True)
        return df
#encoding=utf-8

import numpy as np
import pandas as pd
from log import log
from error_demo.error_code import *
logger = log.log_demo()

class InterpolateCondition:

    def __init__(self, config):
        self.config = config
        # self.is_interpolate = {'PM25':False, 'PM10':False, 'TSP':False, 'SO2':False, 'CO':False, 'NO2':False, 'O3':False}
        self.vars = self.config.get_config_global_data('full_pollutants')
        # 加载TSP判断阈值
        self.pm10_over_pm25_threshold = self.config.get_config_global_data('pm10_over_pm25_threshold')

    def get_is_interpolate_by_var(self, site_df, var):
        self.site_interpolate_limit = self.config.get_config_var_data('site_interpolate_limit', var)
        if var == 'TSP':
            return self.check_is_interpolate_tsp(site_df)
        elif var == 'PM10':
            return self.check_is_interpolate_pm10(site_df)
        else:
            return False


    def check_is_interpolate_tsp(self, site_df_all):
        '''
        1. 根据配置项中该城市是否强制要求插值，如果强制要求，直接return True，否则进入下面的智能判断
        2. 智能判断如果PM10/PM25大于某个阈值（配置项中配置），则return True，否则return False
        '''
        site_df = site_df_all[['SITE_PM25', 'SITE_PM10', 'TIMESTAMP']].copy()
        site_df.dropna(inplace=True)
        if site_df.empty:
            logger.warning("var:TSP 子站最近三小时没有数据，不能进行智能判断！")
            return False

        site_df['PM10_OVER_PM25'] = site_df.apply(lambda x: x.SITE_PM10/x.SITE_PM25, axis=1)

        avg_pm10_over_pm25 = np.mean(site_df['PM10_OVER_PM25'])
        # print("智能判断TSP{}".format(site_df_all['SITE_TSP']))
        mean_tsp = np.mean(site_df_all['SITE_TSP'])

        logger.info('TSP智能判断 avg_pm10_over_pm25: {} mean_tsp {}'.format(avg_pm10_over_pm25, mean_tsp))
        if np.isnan(mean_tsp):
            logger.info("最近三个小时没有TSP数据，不能进行子站插值!")
            return False
        elif avg_pm10_over_pm25 > self.pm10_over_pm25_threshold and mean_tsp > self.site_interpolate_limit:
            return True
        else:
            return False

    def check_is_interpolate_pm10(self, site_df):
        '''
        1. 根据配置项中该城市是否强制要求插值，如果强制要求，直接return True，否则进入下面的智能判断
        2. 智能判断如果PM10/PM25大于某个阈值（配置项中配置），则return True，否则return False
        '''
        site_df = site_df[['SITE_PM25', 'SITE_PM10', 'TIMESTAMP']].copy()
        site_df.dropna(inplace=True)
        if site_df.empty:
            logger.warning("var:PM10 子站最近三小时没有数据，不能进行智能判断！")
            return False

        site_df['PM10_OVER_PM25'] = site_df.apply(lambda x: x.SITE_PM10/x.SITE_PM25, axis=1)

        avg_pm10_over_pm25 = np.mean(site_df['PM10_OVER_PM25'])
        mean_pm10 = np.mean(site_df['SITE_PM10'])

        logger.info('PM10智能判断 avg_pm10_over_pm25: {} mean_pm10 {}'.format(avg_pm10_over_pm25, mean_pm10))
        
        if avg_pm10_over_pm25 > self.pm10_over_pm25_threshold and mean_pm10 > self.site_interpolate_limit:
            return True
        else:
            return False

    # def check_is_interpolate_non_tsp(self, city, var):
    #     '''
    #     根据配置项中该城市是否强制要求插值return True or False
    #     '''
    #     if set(self.interpolate_vars_dict.keys()) >= set(city):
    #         interpolate_vars = self.config.get_config_city_data('interpolate_vars', city[0])
    #         if var in interpolate_vars:
    #             return True
    #         else:
    #             return False
    #     else:
    #         return False

#
# if __name__ == '__main__':
#     config = QualityControlConfig()
#     inter_con = InterpolateCondition(config)
#     site_df = pd.DataFrame()
#     city = [1]
#     var = 'PM25'
#     print(inter_con.get_is_interpolate_by_var(site_df, city, var))
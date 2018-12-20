
import common

import qc_scenario.scenarios as qc
from utility import time_utility as tu
import numpy as np
import math
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class ScenarioDictator():
    def __init__(self, config):
        self.scenarios = qc.QCScenarios
        self.sand_storm_thres = config.get_config_global_data('sand_storm_threshold')
        self.sand_storm_dev_deviation_threshold=config.get_config_global_data('sand_storm_dev_deviation_threshold')
        # self.sand_storm_thres = 1.0
        self.num_hours = 3
        # config.get_config_global_data('num_days_for_sandstorm_training')

    def determine_scenario(self, X, var, hour):
        '''
        为每一种变量确定其是否属于特殊的场景
            case 1: 如果不属于特殊场景，则应return QCScenarios.NORMAL
            case 2: 如果根据config应该插值，则return QCScenarios.INTERPOLATION
            case 3: 如果是扬尘场景，则return QCScenarios.SAND_STORM
        '''
        # print(X)
        cur_hour_df = X
        if cur_hour_df.empty:
            return self.scenarios.INTERPOLATION

        if var == 'PM10':
            hour_n = tu.datetime_n_hours_before_string(
                tu.time_str_to_datetime(hour),
                self.num_hours)
            logger.info('determine_scenario{}'.format(hour_n))
            cur_hour_df = cur_hour_df[cur_hour_df['TIMESTAMP'] > hour_n]
            # cur_hour_df.to_csv('determine_scenario.csv')
            logger.info("determine_scenario:{}".format(str(cur_hour_df['SITE_PM10/SITE_PM25'].mean())))
            logger.info("determine_scenario:{}".format(str(cur_hour_df['PM10/PM25'].mean())))

            # if cur_hour_df['SITE_PM10/SITE_PM25'].mean() >= \
            #    self.sand_storm_thres:
            if ((cur_hour_df['SITE_PM10/SITE_PM25'].mean() >=self.sand_storm_thres)
                and (cur_hour_df['SITE_PM10/SITE_PM25'].mean() - cur_hour_df['PM10/PM25'].mean()) >=self.sand_storm_dev_deviation_threshold) \
                    or ((cur_hour_df['SITE_PM10/SITE_PM25'].mean() >=self.sand_storm_thres) and math.isnan(np.mean(cur_hour_df['PM10/PM25']))):
                return self.scenarios.SAND_STORM

        return self.scenarios.NORMAL

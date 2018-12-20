import time
import numpy as np

import common

from qc_models.qc_model_lr import LinearRegression
from qc_scenario.scenarios import QCScenarios
from qc_scenario.humidity_mode_dictator import HumidityModeDictator, HumidityModeEnum
from utility import time_utility as tu
import os
from log import log
logger = log.log_demo()


class QualityControlDispatcher():
    def __init__(self, config):
        '''
        初始化
        1. self.variables获取需要处理的变量
        2. self.model_names获得要执行的模型名称
        3. self.models用于存放模型
           key_1: var
           key_2: dev_id
           value: 模型对象
        '''
        self.config = config
        self.variables = config.get_config_global_data('full_pollutants')
        self.model_names = {}

        #####~~~~~
        # self.sand_storm_thres = config.get_config_global_data('sand_storm_threshold')
        # self.sand_storm_dev_thres = config.get_config_global_data('sand_storm_dev_deviation_threshold')
        #####~~~~~

        # self.directory = config.get_config_global_data('model_prth_manager')
        # self.directory = '../models/'  # 后边要加配置项
        self.directory = self.config.get_config_global_data('save_path_for_qc_models')
        for var in self.variables:
            self.model_names[var] = config.get_config_var_data('effective_model_list', var)

        self.models = {}
        for var in self.variables:
            self.models[var] = {}

        # self.scenario_dictator = ScenarioDictator(config)
        self.enum_scenarios = QCScenarios
        # print(self.models)

        self.variables_sensitive_to_high_hum = self.config.get_config_global_data('vars_sensitive_to_high_hum')

        self.humidity_dictator = HumidityModeDictator(config)

    def set_qc_lr_parameters(self, pollutant_types):
        self.a_min_normal_hum = self.config.get_config_var_data('qc_lr_model_a_min_normal_hum', pollutant_types)
        self.a_max_normal_hum = self.config.get_config_var_data('qc_lr_model_a_max_normal_hum', pollutant_types)
        self.b_min_normal_hum = self.config.get_config_var_data('qc_lr_model_b_min_normal_hum', pollutant_types)
        self.b_max_normal_hum = self.config.get_config_var_data('qc_lr_model_b_max_normal_hum',pollutant_types)

        logger.info('{} a_min_normal_hum: {}, a_max_normal_hum: {}, b_min_normal_hum: {}, b_max_normal_hum: {}'.format(pollutant_types, self.a_min_normal_hum, self.a_max_normal_hum, self.b_min_normal_hum, self.b_max_normal_hum))

        if pollutant_types in self.variables_sensitive_to_high_hum:
            self.a_min_high_hum = self.config.get_config_var_data('qc_lr_model_a_min_high_hum', pollutant_types)
            self.a_max_high_hum = self.config.get_config_var_data('qc_lr_model_a_max_high_hum', pollutant_types)
            self.b_min_high_hum = self.config.get_config_var_data('qc_lr_model_b_min_high_hum', pollutant_types)
            self.b_max_high_hum = self.config.get_config_var_data('qc_lr_model_b_max_high_hum',pollutant_types)

            logger.info('{} a_min_high_hum: {}, a_max_high_hum: {}, b_min_high_hum: {}, b_max_high_hum: {}'.format(pollutant_types, self.a_min_high_hum, self.a_max_high_hum, self.b_min_high_hum, self.b_max_high_hum))

    def execute_qc_models(self, var, y, X, hour, scenario, city_id):
        '''
        执行对于一个参数配置的所有模型的入口，未来会改成多进程

        Args:
            var: 要处理的参数
            y: 回归变量
            X: Features
            hour: 要产生模型的某个小时
        '''

        # &&& TO-DECIDE: 如何根据场景决定要new什么模型

        device_list = X['DEV_ID'].unique()
        # print('训练数据的一共有多少个设备编号{}'.format(device_list))
        self.set_qc_lr_parameters(var)
        for dev in device_list:
            # self.scenario = self.scenario_dictator.determine_scenario(
            #     y, X, var, hour)
            y_dev = y[y['DEV_ID'] == dev]
            X_dev = X[X['DEV_ID'] == dev]
            # print(self.scenario)

            cur_hour_hum_mode = self.get_cur_hour_hum_mode(dev, X_dev, var, hour)
            logger.debug('###{} current humidity = {}'.format(dev, cur_hour_hum_mode))

            #####~~~~~
            # if scenario == self.enum_scenarios.SAND_STORM:
            #     model = self.new_one_model('scaling', hour=hour)
            # else:
            #####~~~~~

            # t1 = time.time()
            model = self.new_one_model(self.model_names[var], var=var)
            # t2 = time.time()
            # logger.info('new_one_model need {}'.format(t2-t1))
            if var in self.variables_sensitive_to_high_hum:
                model.set_humidity_mode(cur_hour_hum_mode)
            train_X, train_y = self.select_features(X_dev, y_dev, var, scenario)
            # print(train_X)
            model.train(train_X, train_y)
            model.decide_validity(dev)
            #保存模型
            model_file_name = self.get_model_name(model.model_name[0], dev, var, hour, cur_hour_hum_mode)
            model_folder_date = self.get_model_folder_name_by_date(hour)
            model_folder_hour = self.get_model_folder_name_by_hour(hour)
            directory = "{}{}/{}".format(self.directory, model_folder_date, model_folder_hour)
            if not os.path.exists(directory):
                os.makedirs(directory)
            model.save_model(directory, model_file_name)
            # model.restore_model(self.directory, model_file_name)
            logger.warning("城市：{} 质控设备：~~~{} 小时：{} var：{} 模型：{}  参数：{}  有效性: {}".format(city_id, dev, hour, var, self.model_names[var], model.params, model.get_is_valid()))
            # model.save_model('D:/model/', var, dev)
            # TO-IMPLEMENT: 这里要获得所有训练后的模型列表
            self.models[var][dev] = model

    def new_one_model(self, model_name, var=None, hour=None):
        '''
        根据指定的模型名称新建一个模型，这里封装了唯一的不同，也就是类名的不同，将模型返回之后上层采用同样的接口调用即可

        如果为对高湿环境敏感的参数，则传入高湿模式下的判定模型有效的参数，即xxxx_xxx_high_hum系列

        如果为对高湿环境部敏感的参数，则将高湿模式下的参数传入与正常模式相同

        Args:
            model_name: 模型名称

        Returns:
            model: 指定模型名称对应的模型实例
        '''
        # print(model_name)
        if 'lr' in model_name and (var in self.variables_sensitive_to_high_hum):
            return LinearRegression(model_name, self.config, var, self.a_max_normal_hum, self.a_min_normal_hum, self.b_max_normal_hum, self.b_min_normal_hum, self.a_max_high_hum, self.a_min_high_hum, self.b_max_high_hum, self.b_min_high_hum)
        elif 'lr' in model_name and (var not in self.variables_sensitive_to_high_hum):
            return LinearRegression(model_name, self.config, var, self.a_max_normal_hum, self.a_min_normal_hum, self.b_max_normal_hum, self.b_min_normal_hum, self.a_max_normal_hum, self.a_min_normal_hum, self.b_max_normal_hum, self.b_min_normal_hum)
        # elif model_name == 'scaling':
        #     return ScaledBySandstorm(model_name, hour, self.config)
        else:
            logger.info('The model {} is not supported'.format(model_name))
            return None

    def get_models(self):
        return self.models

    def get_cur_hour_hum_mode(self, dev, X, var, target_hour):
        '''
        判断当小时是否处于高湿模式，处于高湿模式时，光散射法的传感器测出来的浓度远远高于实际浓度，因此判断模型是否可信的threshold会发生较大的变化
        '''
        if var in self.variables_sensitive_to_high_hum:
            tmp_df = X[X['TIMESTAMP'] == target_hour]
            if tmp_df.shape[0] > 0:
                hum = np.mean(X[(X['TIMESTAMP'] == target_hour)]['HUMIDITY^1'].values)
                # print('cur_hour = {}, hum = {}'.format(target_hour, hum))
                # print(X[(X['TIMESTAMP'] == target_hour)])
            else:
                prev_hour = tu.str_datetime_to_int_hour_minus_one_hour(target_hour)
                prev_hour = tu.datetime_to_string(prev_hour)
                hum = np.mean(X[(X['TIMESTAMP'] == prev_hour)]['HUMIDITY^1'].values)
                # print('prev_hour = {}, hum = {}'.format(prev_hour, hum))
                # print(X[(X['TIMESTAMP'] == prev_hour)])

            logger.error('dev = {}, var = {}, hum = {}'.format(dev, var, hum))
        else:
            hum = -1
        return self.humidity_dictator.decide_humidity_model(var, humidity=hum)

    def select_features(self, x, y, var, scenario):
        # 处理污染物非PM10的x，y
        # if var in ['PM25', 'SO2', 'CO', 'NO2', 'O3', 'TSP']:
        x = x.drop(['DEV_ID', 'TIMESTAMP'], axis=1)
        y = y.drop(['DEV_ID', 'TIMESTAMP'], axis=1)
        return x, y
        # 污染物为pm10时：沙尘场景、非沙尘场景
        # elif var == 'PM10':
        #     # 处理非沙尘场景：根据阈值过滤掉沙尘天气的PM10数据
        #     if scenario != self.enum_scenarios.SAND_STORM:
        #         y = y.drop(['DEV_ID', 'TIMESTAMP'], axis=1)
        #         x = x.drop(['DEV_ID', 'TIMESTAMP', 'PM25^1', 'PM10/PM25', 'SITE_PM10/SITE_PM25'], axis=1)
        #         return x, y
        #     # 处理沙尘场景的x,y
        #     else:
        #         return x, None

    def get_model_name(self, model_name, dev, var, hour, cur_hour_hum_mode):
        """
        拼接模型保存的名称路径
        :param train_type:
        :param hour:
        :param model_type:
        :return:
        """
        hour_str = tu.extract_number_from_datetime_str(hour)
        if var in self.variables_sensitive_to_high_hum:
            return '{}_{}_{}_{}_{}.pkl'.format(var, dev, hour_str, model_name, cur_hour_hum_mode)
        else:
            return '{}_{}_{}_{}_0.pkl'.format(var, dev, hour_str, model_name)

    def get_model_folder_name_by_date(self, hour):
        """
        拼接模型存储文件名, 一天的文件夹
        :param hour:
        :return:
        """
        date_str = tu.extract_number_from_datetime_str(hour[0:10])
        return "models_{}".format(date_str)

    def get_model_folder_name_by_hour(self, hour):
        """
        拼接模型存储文件名， 一个小时的文件夹
        :param hour:
        :return:
        """
        hour_str = tu.extract_number_from_datetime_str(hour[11:13])
        return "models_{}".format(hour_str)
import datetime
import numpy as np

from utility import time_utility as tu
from utility.distance_utility import weighted_mean_by_distance
from utility import file_utility as fu
from qc_scenario.humidity_mode_dictator import HumidityModeDictator
from qc_models.qc_model_lr import LinearRegression
from qc_models.qc_model_dispatcher import QualityControlDispatcher
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class Transmitter():
    '''
    Transmitter传递器应该是拥有所有的模型，外部给定某个设备的位置和某个参数在某个小时的测量值，则transmitter返回相应的质控后的数据
    '''
    def __init__(self, config, models, qc_version_agent, spatial_indexer):
        '''
        Args:
            config: 如果需要用到配置信息，则进行相应的初始化，否则可以去掉这个入参
            models: 以{var:{ :model}}形式组织的模型存放数据结构，双层字典
            qc_version_agent: 存放aux_entities.vargroup_qc_versions中定义的VargroupQCVersions对象，
            spatial_indexer: 用于存放utility.neigbor_devices中的NeighborDevices对象
        '''
        self.config = config
        self.models = models
        self.qc_version_agent = qc_version_agent
        self.spatial_indexer = spatial_indexer
        self.full_variables = self.config.get_config_global_data('full_pollutants')
        self.directory = self.config.get_config_global_data('save_path_for_qc_models')
        self.humidity_dictator = HumidityModeDictator(config)

        self.variables_sensitive_to_high_hum = self.config.get_config_global_data('vars_sensitive_to_high_hum')

        self.N = {}
        for var in self.full_variables:
            self.N[var] = self.config.get_config_var_data('num_nearest_qc_devices_for_transmission', var)

    def init_model_dispatcher(self):
        self.qcd = QualityControlDispatcher(self.config)

    def control_for_non_qc_device(self, dev_id, vargroup_id, var, X, target_hour, city_id, dev_list=None, is_for_minute=False):
        '''
        1. 找到入参中的设备vargroup_id下参数var对应的质控版本信息
        2. 按照质控版本通过indexer找到对应的质控设备
        3. 在self.models调用相关设备的predict接口，对预测的结果取均值返回

        Args:
            dev_id: 非质控设备id，需要查找他周围的质控设备
            vargroup_id: 需要被指控的设备对应的vargroup_id
            var: 需要进行质控的参数
            X: 当下时刻该设备预测需要的特征
        '''
        #1、根据设备编号，污染物名称 获取对应的版本信息
        version = self.qc_version_agent.get_qc_version_by_vargroup_and_var(vargroup_id, var)

        #2、查找最近的质控设备 字典形式 {dev_id:distance}
        nearest_dev_dict = self.spatial_indexer.find_nearest_qc_devices(dev_id, version, var, self.N[var], False)

        if not nearest_dev_dict:
            logger.warning('城市：{} 时间：{} var:{} ---非质控设备$$${}没有找到最近的质控设备{}'.
                           format(city_id, target_hour, var, dev_id, nearest_dev_dict))
            return None

        nearest_dev = list(nearest_dev_dict.keys())
        logger.warning('城市：{} 时间：{} var:{} ---非质控设备$$${}找到最近的质控设备和距离{}'.
                       format(city_id, target_hour, var, dev_id, nearest_dev_dict))

        #3、在self.models中调用相关设备的predict接口，对预测的结果取均值返回
        count = 0
        distance_list = []
        value_list = []

        if dev_list or is_for_minute:
            #初始化一些判断模型有效性的参数
            # self.set_qc_lr_parameters(var)
            self.init_model_dispatcher()
            self.qcd.set_qc_lr_parameters(var)

        # 获取当小时该设备的湿度模式
        if var in self.variables_sensitive_to_high_hum:
            cur_dev_hour_humidity_mode = self.humidity_dictator.decide_humidity_model(var, humidity=np.mean(X['HUMIDITY^1']))
        else:
            cur_dev_hour_humidity_mode = None

        for i in range(len(nearest_dev)):
            #判断最近的设备在这个时刻是否有模型 且 有效
            cur_distance = nearest_dev_dict[nearest_dev[i]]
            if dev_list is not None or is_for_minute==True:
                # 进入设备清单 传递 模式 或者 分钟级别的质控阶段
                tentative_model = self.parse_model_name(var, nearest_dev[i], target_hour)
                if tentative_model is None:
                    logger.warning(
                        '城市:{} 时间:{}---{}污染物下非质控设备$$${} 附近的质控设备~~~{}没有模型'.format(city_id, target_hour, var, dev_id,nearest_dev[i]))
                    continue
                else:
                    distance_list, value_list, count = self.get_value(X, tentative_model, target_hour, var, dev_id, nearest_dev[i], cur_distance, city_id, cur_dev_hour_humidity_mode, distance_list, value_list, count)
            else:
                # 进入城市传递模式
                if nearest_dev[i] in list(self.models[var].keys()):

                    # 获得旁边设备的模型
                    tentative_model = self.models[var][nearest_dev[i]]
                    distance_list, value_list, count = self.get_value(X, tentative_model, target_hour, var, dev_id, nearest_dev[i], cur_distance, city_id, cur_dev_hour_humidity_mode, distance_list, value_list, count)
                else:
                    logger.warning(
                        "城市：{} 时间：{}---{}污染物下非质控设备$$${} 附近的质控设备~~~{}没有模型".format(city_id, target_hour, var, dev_id, nearest_dev[i]))
                    continue
        if count == 0:
            return None
        else:
            try:
                return weighted_mean_by_distance(2, distance_list, value_list)
            except BaseError as e:
                e.setdata({'报错位置':'传递', 'device_id': dev_id, 'var':var})
                logger.info('设备{}的参数{}找不到有效的质控设备进行模型传递'.format(dev_id, var))
                return None


    # def _get_val_and_distance_list(self, X, var, tentative_model):
    #
    #     if var == 'PM10' and ('lr' in tentative_model.model_name):
    #         X = X.drop(['PM25^1'], axis=1)
    #
    #     return tentative_model.predict(X)

    def _get_val_and_distance_list(self, X, var, tentative_model, distance_list, value_list, count, cur_distance):

        if var == 'PM10' and ('lr' in tentative_model.model_name):
            X = X.drop(['PM25^1'], axis=1)

        value_list.append(tentative_model.predict(X))
        # +1米避免出现距离特别近为0的情况
        distance_list.append(cur_distance + 1)
        count += 1

        return distance_list, value_list, count


    def get_value(self, X, tentative_model, target_hour, var, dev_id, nearest_dev, cur_distance, city_id, dev_hour_humidity_mode,distance_list, value_list, count):
        if tentative_model.get_is_valid():
            if var in self.variables_sensitive_to_high_hum:
                # 判断湿度模式是否匹配
                if dev_hour_humidity_mode == tentative_model.get_humidity_mode():
                    logger.warning(
                        "城市：{} 时间：{}---{}污染物下，非质控设备$$${} 接受了质控设备~~~{}的有效模型，距离{}，当前设备湿度模式{}，相邻设备湿度模式{}".format(city_id, target_hour, var, dev_id, nearest_dev, cur_distance, dev_hour_humidity_mode, tentative_model.get_humidity_mode()))

                    distance_list, value_list, count = self._get_val_and_distance_list(X, var, tentative_model, distance_list, value_list, count, cur_distance)

                else:
                    logger.warning(
                        "城市：{} 时间：{}---{}污染物下，非质控设备$$${} 未能成功接收质控设备~~~{}的有效模型，距离{}，湿度模式不匹配（非质控设备 {} vs 质控设备{}）".format(
                            city_id, target_hour, var, dev_id, nearest_dev, cur_distance,
                            dev_hour_humidity_mode, tentative_model.get_humidity_mode()))

            # 如果无需判断湿度模式
            else:
                logger.warning(
                    "城市：{} 时间：{}---{}污染物下，非质控设备$$${} 接受了质控设备~~~{}的有效模型，距离{}，当前设备湿度模式{}，相邻设备湿度模式{}".format(city_id, target_hour, var, dev_id, nearest_dev, cur_distance, dev_hour_humidity_mode, tentative_model.get_humidity_mode()))
                distance_list, value_list, count = self._get_val_and_distance_list(X, var, tentative_model, distance_list, value_list, count, cur_distance)
        else:
            logger.warning("城市：{} 时间：{}---{}污染物下非质控设备$$${} 附近的质控设备~~~{}的模型无效，距离{}".format(city_id, target_hour, var, dev_id, nearest_dev, cur_distance))
        return distance_list, value_list, count


    def parse_model_name(self, p, dev, hour):
        hour_str = tu.extract_number_from_datetime_str(hour)
        pre_str_dict = self.models
        pre_str_dev = '{}_{}_{}'.format(p, dev, hour_str)
        if pre_str_dev in pre_str_dict.keys():
            model = pre_str_dict[pre_str_dev]
            if model.split('_')[-2] == 'lr':
                model_name = 'lr'
                if p in self.variables_sensitive_to_high_hum:
                    lr = LinearRegression(model_name, self.config, p, self.qcd.a_max_normal_hum, self.qcd.a_min_normal_hum, self.qcd.b_max_normal_hum, self.qcd.b_min_normal_hum, self.qcd.a_max_high_hum, self.qcd.a_min_high_hum, self.qcd.b_max_high_hum, self.qcd.b_min_high_hum)
                else:
                    lr = LinearRegression(model_name, self.config, p, self.qcd.a_max_normal_hum, self.qcd.a_min_normal_hum, self.qcd.b_max_normal_hum, self.qcd.b_min_normal_hum, self.qcd.a_max_normal_hum, self.qcd.a_min_normal_hum, self.qcd.b_max_normal_hum, self.qcd.b_min_normal_hum)
                model_folder_date = fu.get_model_folder_name_by_date(hour)
                model_folder_hour = fu.get_model_folder_name_by_hour(hour)
                directory = "{}{}/{}".format(self.directory, model_folder_date, model_folder_hour)
                lr.restore_model(directory, model)
                return lr
            else:
                print('The requested model to restore is not support!'.format())
        else:
            # logger.info('时间{} var{} 附近的设备{}没有模型！'.format(hour, p, dev))
            return None


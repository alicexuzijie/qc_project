# encoding=utf-8

import common
from utility.distance_utility import weighted_mean_by_distance
from log import log
from error_demo.error_code import *
logger = log.log_demo()

class Interpolation():

    def __init__(self, config, spatial_indexer):
        '''
        初始化，目前想到应该包括的有:
        1. 插值时所需要的最近点个数，用于按照距离平方的导数取反
        '''
        self.config = config
        self.spatial_indexer = spatial_indexer
        self.variables = self.config.get_config_global_data('full_pollutants')
        self.N = {}
        self.init_get_N()

    def init_get_N(self):
        for var in self.variables:
            self.N[var] = self.config.get_config_var_data('num_nearest_devices_for_interpolate', var)

    def interpolation_for_qc(self, city_id, dev_id, df, var, target_hour):
        '''
        1. 给定任意的设备编号，查找其附近的质控设备及非质控设备，拿到距离
        2. 获取临近设备的质控数据，并按照距离的平方倒数为权重进行加权平均
        df : 就是adjust_df
        dev_id : 需要插值的设备编号
        '''
        #1. 给定任意的设备编号，查找其附近的质控设备及非质控设备，拿到距离
        # version = self.qc_version_agent.get_qc_version_by_vargroup_and_var(vargroup_id, var)

        nearest_devices_dict = self.spatial_indexer.find_nearest_devices(dev_id,  var, self.N[var])
        # loger.info('寻找最近设备字典：{}'.format(nearest_devices_dict))
        #给df加索引 加快查找速度
        df_index = df.set_index(['DEV_ID','VAR'])
        if nearest_devices_dict is not None:
            nearest_devices = list(nearest_devices_dict.keys())
            distance_list = []
            value_list = []
            value_df = df[(df['DEV_ID'].isin(nearest_devices)) & (df['VAR'] == var)]
            if value_df.empty:
                logger.debug("城市：{} 时间：{} var：{} 设备^^^{}周围设备都没有数据，因此不能出插值！周围设备编号:{}".format(city_id, target_hour, var, dev_id,nearest_devices))
            else:
                value_nearest_devices = value_df['DEV_ID'].unique().tolist()
                # 2. 获取临近设备的质控数据，并按照距离的平方倒数为权重进行加权平均
                logger.debug("城市：{} 时间：{} ---污染物下{}, 设备^^^{}接受的周围设备编号:{}".format(city_id, target_hour, var, dev_id, nearest_devices))
                for dev in value_nearest_devices:
                    # 距离和值是一一对应的关系
                    if nearest_devices_dict[dev] == 0:
                        distance = nearest_devices_dict[dev] + 1
                    else:
                        distance = nearest_devices_dict[dev]
                    distance_list.append(distance)
                    # value_list.append(list(df[(df['DEV_ID'] == dev) & (df['VAR'] == var)]['ADJ_VALUE'])[0])
                    value_list.append(df_index.loc[(dev, var),'ADJ_VALUE'])

            try:
                interpolate_val = weighted_mean_by_distance(2, distance_list, value_list)
                return interpolate_val
            except BaseError as e:
                e.setdata({'无法插值位置':'临近设备插值', 'dev_id':dev_id, 'var':var})
                logger.info('设备{}的参数{}无法在找到临近设备插值'.format(dev_id, var))
                return None
        else:
            logger.warning('城市：{} 时间：{}---设备^^^{}不出{}参数的数据！'.format(city_id, target_hour, dev_id, var))
            return None



import sys

import common
import config.tools as tools
from error_demo.error_code import *
from log import log
logger = log.log_demo()


class QualityControlConfig(object):
    """
    配置项类，通过实例对象的接口调用获取对应的配置信息
    """
    def __init__(self):
        """
        初始化函数，通过调用sql_data_to_dict函数获取对应配置项字典类型信息，储存在内存
        """
        self.config_var_list = self.get_config_var_list()
        self.config_city_list = self.get_config_city_list()
        self.config_global_list = self.get_config_global_list()
        try:
            self._city = tools.sql_data_to_dict('T_SENSOR_QC_CONFIG_CITY_VAL','T_SENSOR_QC_CONFIG_CITY_ITEM','CITY_ID')
        except BaseError as e:
            e.setdata({'key': '_city'})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
        try:
            self._global = tools.sql_data_to_dict('T_SENSOR_QC_CONFIG_GLOBAL_VAL','T_SENSOR_QC_CONFIG_GLOBAL_ITEM')
        except BaseError as e:
            e.setdata({'key': '_global'})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
        try:
            self._name = tools.var_name_handle('T_DICT_AQ_TYPE')
        except BaseError as e:
            e.setdata({'key': '_name'})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
        try:
            self._var = tools.sql_data_to_dict('T_SENSOR_QC_CONFIG_VAR_VAL','T_SENSOR_QC_CONFIG_VAR_ITEM','VAR_TYPE',self._name)
        except BaseError as e:
            e.setdata({'key': '_var'})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)

    def get_config_global_list(self):
        """
        获取全局配置项列表
        :return: 全局配置项列表
        """
        res = tools.config_data_to_dict('T_SENSOR_QC_CONFIG_GLOBAL_ITEM')
        return res

    def get_config_city_list(self):
        """
        获取城市配置项列表
        :return: 城市配置项列表
        """
        res = tools.config_data_to_dict('T_SENSOR_QC_CONFIG_CITY_ITEM')
        return res

    def get_config_var_list(self):
        """
        获取参数配置项列表
        :return: 参数配置项列表
        """
        res = tools.config_data_to_dict('T_SENSOR_QC_CONFIG_VAR_ITEM')
        return res

    def get_config_global_data(self,config_item):
        """
        获取对应全局配置项的配置值
        :param config_item: 全局配置项
        :return: 对应全局配置项的配置值
        """

        if config_item not in self.config_global_list:
            raise ParameterRangeError('输入的参数不在全局配置项列表中')
        else:
            res=self._global[config_item]
            return res

    def get_config_city_data(self,config_item,city_id):
        """
        获取对应城市配置项的配置值
        :param config_item: 城市配置项
        :param city_id: 城市id
        :return: 对应城市配置项的配置值
        """

        if config_item not in self.config_city_list:
            raise ParameterRangeError('输入的参数不在城市配置项列表中')
        else:
            if city_id not in self._city[config_item].keys():
                raise ParameterRangeError('输入的城市{}不在city级别配置项列表中'.format(city_id))
            else:
                res=self._city[config_item][city_id]
            return res

    def get_config_var_data(self,config_item,var_type):
        """
        获取对应参数配置项的配置值
        :param config_item: 参数配置项
        :param var_type: 参数id
        :return: 对应参数配置项的配置值
        """

        if config_item not in self.config_var_list:
            raise ParameterRangeError('输入的参数不在参数配置项列表中')
        else:
            if var_type not in self._var[config_item].keys():
                raise ParameterRangeError('输入的参数{}不在var级别配置项列表中'.format(var_type))
            else:
                res=self._var[config_item][var_type]
                if config_item == 'effective_model_list':
                    model_list=self.get_config_global_data('model_list')
                    try:
                        tools.data_check(res, model_list)
                    except BaseError as e:
                        e.setdata({'key': 'effective_model_list'})
                        logger.error('code:%s,name:%s,message:%s,data:%s',
                                     e.code, e.name,
                                     e.message, e.getdata(), exc_info=True)
            return res


def main():
    config=QualityControlConfig()
    res=config.get_config_global_list()
    # res=config.get_config_city_list()
    # res=config.get_config_var_list()
    # res=config.get_config_global_data('model_list')
    # res=config.get_config_city_data('is_aqi',1)
    # res=config.get_config_var_data('effective_model_list','CO')
    print(res)


if __name__ == '__main__':
    main()

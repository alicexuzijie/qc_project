import time
from tools import singleton,sql_data_to_dict,config_data_to_dict,data_check,var_name_handle

@singleton
class QualityControlConfig(object):
    """
    配置项类，通过实例对象的接口调用获取对应的配置信息
    """
    def __init__(self):
        """
        初始化函数，通过调用sql_data_to_dict函数获取对应配置项字典类型信息，储存在内存
        """
        self._city = sql_data_to_dict('T_SENSOR_QC_CONFIG_CITY_VAL','T_SENSOR_QC_CONFIG_CITY_ITEM','CITY_ID')
        self._global = sql_data_to_dict('T_SENSOR_QC_CONFIG_GLOBAL_VAL','T_SENSOR_QC_CONFIG_GLOBAL_ITEM')
        self._name = var_name_handle('T_DICT_AQ_TYPE')
        self._var = sql_data_to_dict('T_SENSOR_QC_CONFIG_VAR_VAL','T_SENSOR_QC_CONFIG_VAR_ITEM','VAR_TYPE',self._name)


    def get_config_global_list(self):
        """
        获取全局配置项列表
        :return: 全局配置项列表
        """
        res=config_data_to_dict('T_SENSOR_QC_CONFIG_GLOBAL_ITEM')
        return res

    def get_config_city_list(self):
        """
        获取城市配置项列表
        :return: 城市配置项列表
        """
        res=config_data_to_dict('T_SENSOR_QC_CONFIG_CITY_ITEM')
        return res

    def get_config_var_list(self):
        """
        获取参数配置项列表
        :return: 参数配置项列表
        """
        res=config_data_to_dict('T_SENSOR_QC_CONFIG_VAR_ITEM')
        return res

    def get_config_global_data(self,config_item):
        """
        获取对应全局配置项的配置值
        :param config_item: 全局配置项
        :return: 对应全局配置项的配置值
        """
        config_global_list=self.get_config_global_list()
        try:
            if config_item not in config_global_list:
                raise Exception('{}不是配置项，请输入正确配置项'.format(config_item))
        except Exception as err:
            print('Caught exception: {}'.format(err))
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
        config_city_list=self.get_config_city_list()
        try:
            if config_item not in config_city_list:
                raise Exception('{}不是配置项，请输入正确配置项'.format(config_item))
        except Exception as err:
            print('Caught exception: {}'.format(err))
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
        config_var_list=self.get_config_var_list()
        try:
            if config_item not in config_var_list:
                raise Exception('{}不是配置项，请输入正确配置项'.format(config_item))
        except Exception as err:
            print('Caught exception: {}'.format(err))
        else:
            res=self._var[config_item][var_type]
            if config_item == 'effective_model_list':
                model_list=self.get_config_global_data('model_list')
                data_check(res,model_list)
            return res


def main():
    t1=time.time()
    config=QualityControlConfig()
    # res=config.get_config_global_list()
    # res=config.get_config_city_list()
    # res=config.get_config_var_list()
    # res=config.get_config_global_data('model_list')
    # res=config.get_config_city_data('is_aqi',1)
    # res=config.get_config_var_data('effective_model_list','CO')
    res=config._var
    t2 = time.time()
    print(t2 - t1)
    print(res)



if __name__ == '__main__':
    main()

import pandas as pd
import numpy as np
from error_demo.error_code import *
from log import log
logger = log.log_demo()


class IAQIHandle:
    def __init__(self, config):
        self.config = config
        self.variables = ['PM25','PM10','SO2','CO','NO2','O3']
        self.IAQI_level_one = config.get_config_global_data('IAQI_level_one')
        self.IAQI_level_two = config.get_config_global_data('IAQI_level_two')
        self.IAQI_level_three = config.get_config_global_data('IAQI_level_three')
        self.IAQI_level_four = config.get_config_global_data('IAQI_level_four')
        self.IAQI_level_five = config.get_config_global_data('IAQI_level_five')
        self.IAQI_level_six = config.get_config_global_data('IAQI_level_six')
        self.IAQI_level_seven = config.get_config_global_data('IAQI_level_seven')
        self.IAQI_level_eight = config.get_config_global_data('IAQI_level_eight')
        self.limit_pollution_level_one = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_one')
        self.limit_pollution_level_two = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_two')
        self.limit_pollution_level_three = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_three')
        self.limit_pollution_level_four = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_four')
        self.limit_pollution_level_five = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_five')
        self.limit_pollution_level_six = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_six')
        self.limit_pollution_level_seven = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_seven')
        self.limit_pollution_level_eight = self.get_limit_pollution_dict(self.variables,'limit_pollution_level_eight')

    def get_limit_pollution_dict(self, variables, str):
        """
        通过传入污染物和查找的名称获取对应的配置项
        :param variables: 污染物名称
        :param str: 查找的配置项
        :return: 以污染物为key，对应配置项为value的字典。
        """
        limit_dict = {}
        for var in variables:
            res = self.config.get_config_var_data(str, var)
            try:
                limit_dict[var] = res
            except BaseError as e:
                e.setdata({'key': str, 'var': var})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(),
                             exc_info=True)
        return limit_dict

    def dev_data_by_var(self, df, dev, var):
        """
        通过输入的设备编号和污染物在传进的df中查找对应的值
        :param df: 待处理的df
        :param dev: 目标设备编号
        :param var: 目标污染物
        :return: 对应的数值
        """
        try:
            df.loc[(dev, var), 'ADJ_VALUE']
        except Exception:
            value = np.nan
        else:
            value = df.loc[(dev, var), 'ADJ_VALUE']
        return value

    def query_iaqi(self,dev_value,level_one,level_two,level_three,level_four,level_five,level_six,level_seven,level_eight):
        """
        通过设备本身数值确定区间范围进而获取对应的阈值
        :param dev_value:设备本身数值
        :param level_one:等级一的阈值
        :param level_two:等级二的阈值
        :param level_three:等级三的阈值
        :param level_four:等级四的阈值
        :param level_five:等级五的阈值
        :param level_six:等级六的阈值
        :param level_seven:等级七的阈值
        :param level_eight:等级八的阈值
        :return:返回对应区间范围的阈值
        """
        bp_high = level_two
        bp_low = level_one
        iaqi_high = self.IAQI_level_two
        iaqi_low = self.IAQI_level_one
        if dev_value < level_two:
            bp_high = level_two
            bp_low = level_one
            iaqi_high = self.IAQI_level_two
            iaqi_low = self.IAQI_level_one
        elif dev_value < level_three:
            bp_high = level_three
            bp_low = level_two
            iaqi_high = self.IAQI_level_three
            iaqi_low = self.IAQI_level_two
        elif dev_value < level_four:
            bp_high = level_four
            bp_low = level_three
            iaqi_high = self.IAQI_level_four
            iaqi_low = self.IAQI_level_three
        elif dev_value < level_five:
            bp_high = level_five
            bp_low = level_four
            iaqi_high = self.IAQI_level_five
            iaqi_low = self.IAQI_level_four
        elif dev_value < level_six:
            bp_high = level_six
            bp_low = level_five
            iaqi_high = self.IAQI_level_six
            iaqi_low = self.IAQI_level_five
        elif dev_value < level_seven:
            bp_high = level_seven
            bp_low = level_six
            iaqi_high = self.IAQI_level_seven
            iaqi_low = self.IAQI_level_six
        elif dev_value < level_eight:
            bp_high = level_eight
            bp_low = level_seven
            iaqi_high = self.IAQI_level_eight
            iaqi_low = self.IAQI_level_seven
        return bp_high,bp_low,iaqi_high,iaqi_low

    def query_iaqi_handle(self, adjust_df):
        """
        传入df，查找其中每个设备的iaqi并插入到df中并返回。
        :param adjust_df: 传入待处理的df
        :return: 经过处理加入iaqi的df
        """
        dev_lyst = adjust_df['DEV_ID'].unique()
        df = adjust_df.copy()
        df.set_index(['DEV_ID', 'VAR'], inplace=True)
        devs_lyst = []
        measure_lyst = []
        time_lyst = []
        var_lyst = []
        var_type_id_lyst = []
        value_lyst = []
        mark_lyst = []
        is_normal_lyst = []
        for dev in dev_lyst:
            iaqi_lyst = []
            for var in self.variables:
                dev_value = self.dev_data_by_var(df, dev, var)
                if np.isnan(dev_value):
                    continue
                limit_pollution_level_one = self.limit_pollution_level_one[var]
                limit_pollution_level_two = self.limit_pollution_level_two[var]
                limit_pollution_level_three = self.limit_pollution_level_three[var]
                limit_pollution_level_four = self.limit_pollution_level_four[var]
                limit_pollution_level_five = self.limit_pollution_level_five[var]
                limit_pollution_level_six = self.limit_pollution_level_six[var]
                limit_pollution_level_seven = self.limit_pollution_level_seven[var]
                limit_pollution_level_eight = self.limit_pollution_level_eight[var]
                bp_high, bp_low, iaqi_high, iaqi_low = self.query_iaqi(dev_value,limit_pollution_level_one,limit_pollution_level_two,limit_pollution_level_three,limit_pollution_level_four,limit_pollution_level_five,limit_pollution_level_six,limit_pollution_level_seven,limit_pollution_level_eight)
                iaqi = (iaqi_high-iaqi_low)/(bp_high-bp_low)*(dev_value-bp_low)+iaqi_low
                iaqi_lyst.append(iaqi)
            if not iaqi_lyst:
                continue
            logger.debug('IAQI列表是%s,设备编号：%s'%(iaqi_lyst,dev))
            iaqi_max = np.max(iaqi_lyst)
            dev_df = adjust_df[adjust_df['DEV_ID'] == dev]
            dev_measure_point_id = dev_df['MEA_POINT_ID'].values[0]
            dev_time = dev_df['ADJ_TIME'].values[0]
            dev_mark = 6
            dev_is_normal = dev_df['IS_NORMAL'].values[0]
            devs_lyst.append(dev)
            measure_lyst.append(dev_measure_point_id)
            value_lyst.append(iaqi_max)
            var_lyst.append('AQI')
            var_type_id_lyst.append(7)
            time_lyst.append(dev_time)
            mark_lyst.append(dev_mark)
            is_normal_lyst.append(dev_is_normal)
        dict = {'DEV_ID':devs_lyst,'MEA_POINT_ID':measure_lyst,'ADJ_VALUE':value_lyst,'VAR':var_lyst,'VAR_TYPE_ID':var_type_id_lyst,'ADJ_TIME':time_lyst,'MARK':mark_lyst,'IS_NORMAL':is_normal_lyst}
        iaqi_df = pd.DataFrame(dict)
        return iaqi_df


if __name__ == '__main__':
    from config.qc_config import QualityControlConfig
    config = QualityControlConfig()
    df = pd.read_csv('adjust_df_4.csv')
    iaqi = IAQIHandle(config)
    res = iaqi.query_iaqi_handle(df)
    res.to_csv('res.csv')

from enum import Enum
from error_demo.error_code import *
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class HumidityModeEnum(Enum):
    NORMAL = 1
    EXTREMELY_HIGH = 2


class HumidityModeDictator():

    def __init__(self, config):
        self.config = config
        self.humidity_modes = HumidityModeEnum

        self.variables = self.config.get_config_global_data('vars_sensitive_to_high_hum')

        # 初始化高湿和低湿场景划分的cutting point
        self.high_low_humidity_cutting = {}
        for var in self.variables:
            try:
                high_low_humidity_cutting = self.config.get_config_var_data("high_low_humidity_cutting", var)
                self.high_low_humidity_cutting[var] = high_low_humidity_cutting
            except ParameterRangeError as e:
                e.setdata({'查询参数':var, '查询项':'high_low_humidity_cutting'})
                logger.error('code:%s,name:%s,message:%s,data:%s', e.code, e.name, e.message, e.getdata(), exc_info=True)

    def decide_humidity_model(self, var, humidity=-1):
        '''
        如果variable不是PM25，就不需要用户传入humidity了，默认为-1，return为NORMAL
        '''
        if var in self.variables:
            if humidity > self.high_low_humidity_cutting[var]:
                return self.humidity_modes.EXTREMELY_HIGH.value

        return self.humidity_modes.NORMAL.value

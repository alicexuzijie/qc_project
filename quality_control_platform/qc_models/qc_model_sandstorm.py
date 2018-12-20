import common
from qc_models.qc_model_scaling import ScaledByConstant
from qc_scenario.humidity_mode_dictator import HumidityModeDictator, HumidityModeEnum
from utility import time_utility as tu
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class ScaledBySandstorm(ScaledByConstant):
    def __init__(self, model_name, hour, config):
        super().__init__(model_name)
        self.num_hours = 3
        self.hour = hour
        self.params_name = 'SITE_PM10/SITE_PM25'
        self.var_pollutant = 'PM25^1'

    def prepare_train(self, features):
        if features.empty:
            logger.info('训练数据为空')
        else:
            hour_n = tu.datetime_n_hours_before_string(
                tu.time_str_to_datetime(self.hour),
                self.num_hours)
            features = features[features['TIMESTAMP'] >= hour_n]
            if features.empty:
                logger.info('训练数据为空')
                return None
            else:
                return features

    def train(self, features, y=None):
        try:
            if features is None:
                logger.info('沙尘场景没有features数据')
            else:
                features = self.prepare_train(features)
                super().train(features, self.params_name)
            # print("super.params",self.params)
        except Exception as e:
            logger.error('erro')

    def predict(self, features):
        try:
            adj_value = super().predict(features, self.var_pollutant)
            return adj_value
        except Exception as e:
            logger.error('erro')

    def get_is_valid(self):
        return self.is_valid

    def set_humidity_mode(self, var, hum=-1):
        self.cur_hour_humidity_mode = HumidityModeEnum.NORMAL

    def get_humidity_mode(self):
        return self.cur_hour_humidity_mode

    def save_model(self, directory, model_file_name):
        super().save_model(directory, model_file_name)

    def restore_model(self, directory, model_file_name):
        super().restore_model(directory, model_file_name)
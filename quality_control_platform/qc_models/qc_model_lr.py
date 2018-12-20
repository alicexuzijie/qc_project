#encoding=utf-8
import os
from sklearn.linear_model import LinearRegression as LR
from sklearn.externals import joblib
from qc_models.qc_model import QualityControlModel
from qc_scenario.humidity_mode_dictator import HumidityModeEnum

from log import log
from error_demo.error_code import *
logger = log.log_demo()


class LinearRegression(QualityControlModel):

    def __init__(self, model_name, config, pollutant_types, a_max_normal_hum, a_min_normal_hum, b_max_normal_hum, b_min_normal_hum, a_max_high_hum, a_min_high_hum, b_max_high_hum, b_min_high_hum):
        super(LinearRegression, self).__init__(model_name)
        self.params = []
        self.model = None

        self.a_max_normal_hum = a_max_normal_hum
        self.a_min_normal_hum = a_min_normal_hum
        self.b_max_normal_hum = b_max_normal_hum
        self.b_min_normal_hum = b_min_normal_hum

        self.a_max_high_hum = a_max_high_hum
        self.a_min_high_hum = a_min_high_hum
        self.b_max_high_hum = b_max_high_hum
        self.b_min_high_hum = b_min_high_hum

        self.have_consistency_model_pollutant = \
            config.get_config_global_data('have_consistency_model_pollutant')
        if pollutant_types in self.have_consistency_model_pollutant:
            self.is_valid = False
        else:
            self.is_valid = True

        self.humidity_modes = HumidityModeEnum
        self.cur_hour_hum_mode = self.humidity_modes.NORMAL

    def train(self, features, y):
        self.model = LR()
        self.model.fit(features, y)
        self.set_params()

        # print("Model training finished.")

    def set_params(self):
        self.params = list(self.model.coef_[0]) + list(self.model.intercept_)

    def predict(self, features):
        try:
            prediction = self.model.predict(features)
            return prediction[-1][0]
        except Exception as e:
            logger.error("ERROR:{}\n Try to train the model first.".format(e))

    def decide_validity(self, dev):
        '''
        不同的湿度模式下用不同的参数判断是否为valid
        '''
        if self.cur_hour_hum_mode == self.humidity_modes.NORMAL.value:
            self._decide_validity_by_para(dev, self.a_max_normal_hum, self.a_min_normal_hum, self.b_max_normal_hum, self.b_min_normal_hum)
        else:
            self._decide_validity_by_para(dev, self.a_max_high_hum, self.a_min_high_hum, self.b_max_high_hum, self.b_min_high_hum)

    def _decide_validity_by_para(self, dev, a_max, a_min, b_max, b_min):
        '''
        通用函数，给定任何一个a, b min_max套装，判断是否valid
        '''
        if self.params[0] <= a_max and self.params[0] >= a_min and self.params[-1] <= b_max and self.params[-1] >= b_min and self.is_valid == False:
            self.is_valid = True
        else:
            self.is_valid = False

        logger.info('!!!{} is validity is {}. a_min = {}, a_max = {}, b_min = {}, b_max = {}'.format(dev, self.is_valid, a_min, a_max, b_min, b_max))

    def save_model(self, directory, model_file_name):
        try:
            address = directory + "/" +model_file_name
            if not os.path.exists(directory):
                os.makedirs(directory)
            joblib.dump(self.model, address)
            logger.info("Model successfully saved")
        except Exception as e:
            logger.error("ERROR:{}\n Model saving failed.".format(e))

    def restore_model(self, directory, model_file_name):
        try:
            #解析文件名中的湿度模式
            self.cur_hour_hum_mode = int(model_file_name.split('_')[-1][:-4])
            dev = model_file_name.split('_')[1]
            # self.model_name = 'lr'
            address = directory + "/" +model_file_name
            self.model = joblib.load(address)
            # self.params = list(self.model.coef_[0]) + list(self.model.intercept_)
            self.set_params()
            self.decide_validity(dev)
            logger.info("Model successfully loaded.")
        except Exception as e:
            logger.error("ERROR:\n Model loading failed.".format(e))

    def get_params(self):
        return self.params

    def get_is_valid(self):
        return self.is_valid

    def set_humidity_mode(self, cur_hour_hum_mode):
        '''
        设置当小时湿度模式
        '''
        self.cur_hour_hum_mode = cur_hour_hum_mode

    def get_humidity_mode(self):
        '''
        返回当小时湿度模式
        '''
        return self.cur_hour_hum_mode

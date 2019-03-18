
import common
from qc_models.qc_model import QualityControlModel
from sklearn.externals import joblib
import pandas as pd
import os
from log import log
from error_demo.error_code import *
logger = log.log_demo()

class ScaledByConstant(QualityControlModel):

    def __init__(self, model_name):
        super().__init__(model_name)
        self.params = None

    def decide_validity(self, dev):
        if self.params is not None:
            self.is_valid = True
        else:
            self.is_valid = False
        return self.is_valid

    def train(self, features, params_name, y=None):
        try:
            self.params = features[params_name].mean()
        except Exception as e:
            logger.error("Model training finished.")

    def predict(self, features, var_pollutant):
        try:
            # print('features',features)
            # print("var_pollutant",var_pollutant)
            # print('self.params',self.params)
            # print("features[var_pollutant].mean()",features[var_pollutant].mean())
            adj_value = features[var_pollutant].mean() * self.params
            # print(adj_value)
            return adj_value
        except Exception as e:
            logger.error("ERROR:{} \n Try to train the model first.".format(e))

    def save_model(self, directory, model_file_name):
        try:
            address = address = directory + "/" +model_file_name
            if not os.path.exists(directory):
                os.makedirs(directory)
            if self.params is not None:
                self.params.pd.to_csv(address)
                logger.info("Model successfully saved")
            else:
                logger.info("Maybe no hour data so Model failed saved")
        except Exception as e:
            logger.error("ERROR:{}\n Model saving failed.".format(e))

    def restore_model(self, directory, model_file_name):
        try:
            # 解析文件名中的湿度模式
            cur_hour_hum_mode = int(model_file_name.split('_')[-1][:-4])
            dev = model_file_name.split('_')[1]
            self.set_humidity_mode(cur_hour_hum_mode)
            # self.model_name = 'scaling'
            address = directory + "/" + model_file_name
            params = pd.read_csv(address)
            self.params = params.loc[0][0]
            self.decide_validity(dev)
            logger.info("Model successfully loaded.")
        except Exception as e:
            logger.error("ERROR:{}\n Model loading failed.".format(e))

    def get_params(self):
        return self.params

    def set_humidity_mode(self, var, hum=-1):
        pass

    def get_humidity_mode(self):
        pass


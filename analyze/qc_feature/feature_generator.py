import numpy as np
import pandas as pd

import common
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class FeatureManager():
    '''
    本类针对不同的污染物根据配置项产生特征，并且组织为y, X的形式进行返回
    '''
    def __init__(self, config):
        '''
        将config中关于feature的内容解析成feature_entity_dict
            key: variable
            value: list<FeatureEntity>
        '''
        self.variables = config.get_config_global_data('full_pollutants')
        self.feature_entity_dict_train = {}
        self.feature_entity_dict_for_prediction = {}
        self.workable_variables = []

        for var in self.variables:

            try:
                feature_strings = config.get_config_var_data('selected_feature_list', var)
                feature_strings_for_prediction = config.get_config_var_data('non_qc_selected_feature_list', var)
                feature_entity = []
                feature_entity_prediction = []
                for i in range(len(feature_strings)):
                    feature_entity.append(FeatureEntity(feature_strings[i], var))
                self.feature_entity_dict_train[var] = feature_entity

                for i in range(len(feature_strings_for_prediction)):
                    feature_entity_prediction.append(FeatureEntity(feature_strings_for_prediction[i], var))
                self.feature_entity_dict_for_prediction[var] = feature_entity_prediction
                self.workable_variables.append(var)
            except ParameterRangeError as e:
                e.setdata({'查询项':'selected_feature_list or non_qc_selected_feature_list'})
                logger.error('查询项{}在var级别配置项中不存在'.format(var))

        self.gen_feature_names()
        # print(self.feature_entity_dict_for_prediction.keys())

    def gen_feature_names(self):
        self.feature_names = {}
        for var in self.workable_variables:
            # print('Variable {} has {} features...'.format(var, len(self.feature_entity_dict_train[var])-1))
            f_names = []
            for i in range(1, len(self.feature_entity_dict_train[var])):
                f_names.append(self.feature_entity_dict_train[var][i].get_x_name())
            self.feature_names[var] = f_names

        self.feature_names_for_prediction = {}
        for var in self.workable_variables:
            # print('Variable {} has {} features...'.format(var, len(self.feature_entity_dict_for_prediction[var]) - 1))
            f_names_prediction = []
            for i in range(1, len(self.feature_entity_dict_for_prediction[var])):
                f_names_prediction.append(self.feature_entity_dict_for_prediction[var][i].get_x_name())
            self.feature_names_for_prediction[var] = f_names_prediction

    def get_feature_names_by_var(self, var, is_train = False):
        '''
        Returns:
            特征变量列表：list<string>
        '''
        if is_train:
            return self.feature_names[var]
        else:
            return self.feature_names_for_prediction[var]

    def get_prepared_data_for_train(self, df, var):
        '''
        根据给定的dataframe和要处理的参数var，给出y和X（其中X是矢量）

        Args:
            df: 经过处理的org数据
            var: 要质控的参数
        Returns:
            y: 质控设备绑定的子站数据
            X: features
        '''
        y = self.feature_entity_dict_train[var][0].get_y_val(df)
        X = self.get_prepared_data_for_train_X(df, var)
        return y, X

    def get_prepared_data_for_prediction(self, df, var):
        '''
        根据给定的dataframe和要处理的参数var，给出矢量X

        Args:
            df: 经过处理的org数据
            var: 要质控的参数
        Returns:
            X: features
        '''
        n_features = len(self.feature_entity_dict_for_prediction[var])
        # print(n_features)

        X = df[['DEV_ID', 'TIMESTAMP']]
        for cnt_f in range(1, n_features):
            tmp_X = self.feature_entity_dict_for_prediction[var][cnt_f].get_x_val(df)
            X = pd.concat([X, tmp_X], axis=1)

        return X

    def get_prepared_data_for_train_X(self, df, var):
        '''
        根据给定的dataframe和要处理的参数var，给出矢量X

        Args:
            df: 经过处理的org数据
            var: 要质控的参数
        Returns:
            X: features
        '''
        n_features = len(self.feature_entity_dict_train[var])
        # print(n_features)

        X = df[['DEV_ID', 'TIMESTAMP']]
        for cnt_f in range(1, n_features):
            tmp_X = self.feature_entity_dict_train[var][cnt_f].get_x_val(df)
            X = pd.concat([X, tmp_X], axis=1)

        return X


class FeatureEntity():
    def __init__(self, feature_string, var):
        '''
        将feature_string，例如y-power-1拆分成3个元素
            variable: belongs to {x, y, HUMIDITY, TEMPERATURE}
            operator: power, log
            param: power的指数或者log的底数

        Args:
            feature_string: 某一个feature string，例如y-power-1, 或者 x-power-2
            var: 这个entity对应的污染物
        '''
        self.feature_string = feature_string
        self.var = var

        elements = feature_string.split('-')
        if elements[0] == 'y':
            self.is_y = True
        else:
            self.is_y = False

        self.variable = elements[0]
        self.operator = elements[1]
        self.param = elements[2]

        if self.is_y:
            self.y_name = self.get_y_name()
        else:
            self.x_name = self.get_x_name()

    def get_y_name(self):
        '''
        如果本entity对应的是y，则给出y的变量名，目前的命名分为两种情况：
            case 1: power-1，则代表本名，不做变换。得到的变量名诸如SITE_PM25, SITE_PM10
            case 2: log-e，则代表对y做ln变换，得到的变量名为LOG_SITE_PM25, LOG_SITE_PM10等

        Returns:
            变量名: string
        '''
        if self.is_y:
            if self.operator == 'power':
                return 'SITE_{}'.format(self.var)
            elif self.operator == 'log':
                return 'LOG_SITE_{}'.format(self.var)
            else:
                logger.debug('The operator {} is not supported'.format(self.operator))
        else:
            # $$$做容错
            logger.debug('The feature is not for y')

    def get_x_name(self):
        if self.operator == 'power':
            return '{}^{}'.format(self.variable, self.param)
        elif self.operator == 'over':
            return '{}/{}'.format(self.variable, self.param)
        else:
            # $$$做容错
            logger.debug('The operator {} is not supported'.format(self.operator))


    def get_y_val(self, df):
        '''
        获得y值

        Args:
            df: 原dataframe，由org数据生成

        Returns:
            df[y_name]: 以y_name命名的单列dataframe，其变换方式由self.operator指定
        '''
        if self.is_y:
            var_df = df[['SITE_{}'.format(self.var),
                         'DEV_ID', 'TIMESTAMP']].copy()
            if self.operator == 'log':
                # 注意这里还没有对为0的情况进行处理，可能是在前面就drop掉相应的行
                var_df[self.y_name] = np.log(var_df['SITE_{}'.format(self.var)])
            return var_df[[self.y_name, 'DEV_ID', 'TIMESTAMP']]
        else:
            # $$$做容错
            logger.debug('This feature entity is not for y.')

    def get_x_val(self, df):
        '''
        获得本entity对应的某一列x值

        Args: 
            df: dataframe，由org数据生成
        
        Returns:
            df[x_name]：以x_name命名的单列dataframe，其变换方式由self.operator指定
        '''
        # logger.info(self.to_string())
        if self.is_y:
            logger.debug('This is not for generate feature but for y transformation')
        else:
            if self.operator == 'power':
                var_df = df[[self.variable]].copy()
                var_df[self.x_name] = np.power(var_df[self.variable], int(self.param))
                return var_df[[self.x_name]]
            elif self.operator == 'over':
                var_df = df[[self.variable, self.param]].copy()
                # 注意这里还没有对为0的情况进行处理，可能是在前面就drop掉相应的行
                var_df[self.x_name] = var_df[self.variable]/var_df[self.param]
                return var_df[[self.x_name]]


    def to_string(self):
        return self.feature_string

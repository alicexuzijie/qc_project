import common
from config.qc_config import QualityControlConfig
from utility.neighbor_devices import NeighborDevices
from utility import time_utility as tu
from utility import file_utility as fu
from utility.iaqi_utility import IAQIHandle
from qc_prepare.org_filter import OrgFilter
from qc_prepare.org_sampler import OrgSampler
from qc_feature.feature_generator import FeatureManager
from qc_scenario.scenario_functions import ScenarioDictator
from qc_models.qc_model_dispatcher import QualityControlDispatcher
from qc_transmission.transmission import Transmitter
from aux_entities.vargroup_qc_versions import VargroupQCVersions
from aux_entities.vargroup_channels import VargroupChannels
from qc_interpolate.qc_interpolate import Interpolation
from qc_censor.data_check import DataCheck
from qc_site_interpolate.site_interpolate import SiteInterpolation
from qc_site_interpolate.interploate_condition import InterpolateCondition
import datetime
import numpy as np
import pandas as pd
import os
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class QualityControlRoutine():
    """
    ???? 整体逻辑还差一个把所有的数据组装成dataframe以便写入数据库
    """
    def __init__(self, dao):
        '''
        整体质控逻辑的主要入口，在这里初始化绝大多数的对象，可以传递给底层的模块进行使用
        '''
        # 可能潜在处理的参数
        self.config = QualityControlConfig()
        self.dao = dao
        self.org_f = OrgFilter(self.config)
        self.org_s = OrgSampler(self.config)
        self.feature_gen = FeatureManager(self.config)
        self.scenario_agent = ScenarioDictator(self.config)
        self.model_dispatcher = QualityControlDispatcher(self.config)
        self.vargroup_qc_version = VargroupQCVersions(self.dao)
        self.iaqi = IAQIHandle(self.config)
        # channel_df = self.dao.query_channels()
        # aq_dict = self.dao.query_aq_type_in_dict()
        # self.var_names_by_vargroup = VargroupChannels(channel_df, aq_dict)
        #初始化判断是否进行子站插值类
        self.inter_condition = InterpolateCondition(self.config)
        self.variables = self.config.get_config_global_data('full_pollutants')
        self.full_vars = self.variables
        self.directory = self.config.get_config_global_data('save_path_for_qc_models')
        # self.directory = '../models/'  # 后边要加配置项
        self.direct_org_city_list = self.config.get_config_global_data('direct_org_city_list') #需要出org数据的
        self.interpolate_vars_dict = self.config._city["interpolate_vars"]
        self.aqi_city_list = self.config.get_config_global_data('aqi_city_list')

        #存储每个污染物下需要邻域插值的设备编号
        self.no_data_devices = {}

        # 字典，存储不同方式出来的质控设备
        # key - mark
        # value - adjust dataframe
        # mark编码含义描述
        # 0 - 质控设备直接拟合出的数据或者直接由子站赋值
        # 1 - 非质控设备：接受质控设备的模型传递出来的质控数据
        # 2 - 非质控设备：因为配置要求出插值数据，或者配置要求满足某一项条件、且经检查满足后出子站插值数据
        # 3 - 非质控设备：未能成功接收质控模型，被邻域小设备插值的质控数据
        # 4 - 非质控设备：未能成功找到相邻设备，被子站插值的质控数据
        # 5 - 非质控设备：由于项目刚启动，可能直接要求从org数据中format为adjust数据
        # 6 - 非质控设备：计算每个设备的AQI

        self.all_adjust_df = {}
        for mark in range(7):
            self.all_adjust_df[mark] = self.new_adjust_df_header()

        # self.adjust_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])
        # self.transmission_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])
        # self.interpolate_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])
        # self.adjust_df_full = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])
        # self.site_interpolate_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])

        self.qc_device_tabu_dict = {}
        self.nonqc_device_transmission_tabu_dict = {}
        for var in self.variables:
            try:
                self.qc_device_tabu_dict[var] = self.config.get_config_var_data('qc_device_tabu_list', var)
            except ParameterRangeError as e:
                e.setdata({'查询项':'qc_device_tabulist', '查询var':var})
                logger.warning('查询的配置项不存在')
            try:
                self.nonqc_device_transmission_tabu_dict[var] = self.config.get_config_var_data('nonqc_device_transmission_tabu_list', var)
            except ParameterRangeError as e:
                e.setdata({'查询项':'nonqc_device_transmission_tabu_list', '查询var':var})
                logger.warning('查询的配置项不存在')

    def init_spatial_indexer(self, city_id):
        self.device_list = []
        self.dev_measure_point_id_dict = {}
        self.dic_city = self.dao.query_workable_vars_by_city_list(city_id)
        # 1. 查询出该城市的所有设备，并用该城市的设备初始化该城市的spatial indexer用于查询最近的设备
        self.device_list_info = self.dao.query_active_devices_by_city(city_id)
        self.dev_measure_point_id_dict = self.df_dict()
        self.device_list = self.device_list_info['DEV_ID'].unique().tolist()
        logger.info("城市{}下共有{}个设备".format(city_id,len(self.device_list)))
        self.spatial_indexer = NeighborDevices(self.dao, self.device_list, city_id)

    def init_site_interpolate(self, hour):
        self.site_inter = SiteInterpolation(self.dao, hour, self.spatial_indexer, self.device_list_info, self.dev_measure_point_id_dict)

    def init_qc_data_min_max(self):
        """
        获取var级别的配置项放到内存里
        :return:
        """
        self.min_qc_values = {}
        self.max_qc_values = {}
        self.tabu_percent = {}
        for var in self.variables + ['TVOC']:
            self.min_qc_values[var] = self.config.get_config_var_data("min_qc_value", var)
            self.max_qc_values[var] = self.config.get_config_var_data("max_qc_value", var)
            self.tabu_percent[var] = self.config.get_config_var_data('tabu_percent', var)

    def init_check(self, city_id, hour):
        # 初始化审核函数
        self.qc_censor = DataCheck(self.dao, self.config, self.spatial_indexer, self.device_list_info, city_id, hour, self.variables)

    def init_and_categorize_vars(self, city_id):
        '''
        对要处理的参数进行分类，包括需要质控的参数，潜在可能需要插值的参数和必须插值的参数
        '''
        # 初始化要质控的污染物
        self.qc_variables = self.variables
        self.need_interpolate_variables = []
        self.potential_need_interpolate_variables = [] #需要智能判断确定需要进行插值的列表
        # 获取必须进行插值的配置项  注意city_id可能是一个列表
        if len(city_id) == 1 and (city_id[0] in self.interpolate_vars_dict.keys()):
            self.must_interpolate_variables = self.interpolate_vars_dict[city_id[0]]
            if '' in self.must_interpolate_variables:
                self.must_interpolate_variables.remove('')
        else:
            self.must_interpolate_variables = []

        # 初始化可能要进行子站插值的污染物，如果没有报错则说明有potential error，赋值为[]
        try:
            self.potential_interpolate_variables = self.config.get_config_global_data('potential_interpolate_vars')
            if '' in self.potential_interpolate_variables:
                self.potential_interpolate_variables.remove('')
        except BaseError as e:
            logger.error(e)
            self.potential_interpolate_variables = []

    def init_site_data(self, city_id, hour):
        self.site_df = self.dao.query_site_train_data_by_city(city_id, hour, is_cache=True)
        #子站数据进行去重
        self.drop_duplicate()
        self.site_one_hour_df = self.site_df[self.site_df['TIMESTAMP']==hour].copy()
        self.site_before_hours_df = self.site_df[self.site_df['TIMESTAMP']<hour].copy()

        # 把前三个小时的数据做平均，并且保存下来，如果self.site_one_hour_df为空插值时则考虑self.site_three_hours_df
        two_hours_before = tu.str_datetime_n_hours_before_string(hour, 2)
        self.site_three_hours_df = self.site_df[(self.site_df['TIMESTAMP'] <=  hour) & (self.site_df['TIMESTAMP'] >= two_hours_before)]
        self.site_three_hours_df.drop(columns=['TIMESTAMP'], inplace=True)
        tmp_grp = self.site_three_hours_df.groupby(['SITE_ID']).mean()
        tmp_grp.reset_index(inplace=True)
        self.site_three_hours_df = tmp_grp
        self.site_three_hours_df['TIMESTAMP'] = self.site_three_hours_df.apply(lambda x: hour, axis=1)

    def drop_duplicate(self):
        self.site_df = self.site_df.groupby(['SITE_ID', 'TIMESTAMP']).first()
        self.site_df.reset_index(inplace=True)

    def new_adjust_df_header(self):
        return pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])

    def obtain_adjust_data(self, city_id, hour):
        '''
        从ORG数据获取质控数据，分为几种情况：
        1. 城市要求直接出原始ORG数据
        2. 城市要求某些变量插值
        3. 从全局看某些变量需要智能判断是否需要出插值
        4. 正常走训练质控模型 - 传递质控模型 - 紧邻插值的逻辑

        Args:
            city: list, 要处理的城市列表
            hour: timestamp including hour, 要处理的时间
        '''
        # 初始化质控后数据不同参数的最大值和最小值
        self.init_qc_data_min_max()
        # 初始化self.spatial_indexer类
        self.init_spatial_indexer(city_id)
        # 初始化子站插值
        self.init_site_interpolate(hour)
        # 初始化和分类要处理的参数
        self.init_and_categorize_vars(city_id)
        # 初始化审核
        self.init_check(city_id, hour)
        #初始化子站数据
        self.init_site_data(city_id, hour)

        # 如果要求该城市直接出原始数据，则直接从ORG取数到ADJUST表，注意，此时不做审核和插值，因为在项目初期发现问题更重要
        if set(self.direct_org_city_list) >= set(city_id):
            self.direct_org_to_adjust(city_id, hour)
            return

        #对当小时的子站数据进行审核并进行审核，将异常的子站数据审核掉
        self.execute_site_data_censor()

        # 处理需要子站数据插值的情况
        self.process_site_interpolation_by_city(city_id, hour)

        # 如果插值后所有变量被处理完毕，直接return
        if len(self.qc_variables) == 0:
            #进行质控的vars 如果为空 直接结束执行
            return

        # 否则进入正常的质控
        self.execute_train_transmission_by_city(city_id, hour)

        # 对质控后的数据筛选没有出数的进行子站插值
        self.execute_site_interpolate_by_devices(hour, city_id)

        if set(self.aqi_city_list) >= set(city_id):
            #该城市对每个设备添加AQI
            self.execute_add_aqi()

        # 对所有的df经过最大值和最小值处理替换后入库
        self.write_all_adjust_df_to_db(hour)

    def execute_add_aqi(self):
        all_adjust_df_temp = self.concat_adjust_df()
        self.all_adjust_df[6] = self.iaqi.query_iaqi_handle(all_adjust_df_temp)

    def direct_org_to_adjust(self, city_id, hour):
        #城市要求直接出org数据的情况
        #因为都是list类型 所以改用这种比较方式 当[city_id]是[direct_org_city_list]子集 的时候
        #直接把org数据转换成adjust
        logger.info('城市{} 时间{} 进入到format_org_to_adjust阶段。。。。。'.format(city_id, hour))
        org_df = self.dao.query_org_data_by_device_list(self.device_list, hour, hour)
        self.drop_columns(org_df)
        self.format_org_to_adjust(org_df, hour, self.full_vars)
        return

    def process_tvoc_direct_org_to_adjust(self, hour):
        #如果某城市下tvoc
        print('开始处理TVOC。。。')
        org_df_by_city = pd.concat([self.qc_dev_df, self.non_qc_dev_df], axis=0)
        var = ['TVOC']
        # org_df_by_city_temp = org_df_by_city[['DEV_ID', var]].copy()
        # org_df_by_city_temp.dropna(inplace=True)
        if len(org_df_by_city) > 0:
            self.format_org_to_adjust(org_df_by_city, hour, var)

    def determine_site_df_to_interpolate(self, city_id):
        '''
        本函数决定具体用最近一个小时的子站数据插值还是用最近三个小时的子站数据插值
        '''
        num_site_one_hour = len(self.site_one_hour_df['SITE_ID'].unique())
        num_site_three_hours = len(self.site_three_hours_df['SITE_ID'].unique())
        if num_site_three_hours > 0:
            if float(num_site_one_hour) / num_site_three_hours > 0.95:
                logger.warning('s^s^s^{}城市成功用当小时数据启动插值'.format(city_id))
                return self.site_one_hour_df
            else:
                logger.warning('s^s^s^{}城市成功用最近三小时数据启动插值'.format(city_id))
                return self.site_three_hours_df
        else:
            logger.error('s^s^s^{}城市未能成功用最近三小时的数据插值，请检查子站数据是否缺失'.format(city_id))
            return pd.DataFrame()

    def process_site_interpolation_by_city(self, city_id, hour):
        '''
        处理在需要子站插值的情况，包括必须插值和需要经过判断插值两种情况

        Args:
            city_id: 需要处理的城市清单
            hour: 需要处理的timestamp
        '''

        # 插值前清空中间数据
        self.site_inter.clear_inter_site_df()

        # 如果子站当小时没有数据，则检查最近三小时是否有数据
        # 如果最近三小时都没有数据，则不进行子站数据审核
        cur_site_df = self.determine_site_df_to_interpolate(city_id)
        if cur_site_df.empty:
            return

        mark = 2

        # 城市要求必须要插值的情况
        if len(self.must_interpolate_variables) > 0:
            # 将必须插值的污染物参数放到带插值的参数中
            self.need_interpolate_variables = self.must_interpolate_variables

        # 智能判断可能需要插子站值的情况
        if len(self.potential_interpolate_variables) > 0:
            difference_set_vars = list(set(self.potential_interpolate_variables) - set(self.must_interpolate_variables))
            if len(difference_set_vars) > 0:
                # 可能需要进行插值的potential_interpolate_variables 不为空的话 且 刚才没有进行子站插值的话 update qc_variables
                self.process_potential_site_interpolate(city_id, hour, difference_set_vars)

        if len(self.need_interpolate_variables) > 0:
            self.all_adjust_df[mark] = self.site_inter.execute_site_interpolate(city_id, hour, self.need_interpolate_variables, cur_site_df, 2)
            logger.warning("s^s^s^{}城市所有需要第一轮子站插值的vars: {}".format(city_id, self.need_interpolate_variables))
        else:
            logger.warning("s^s^s^{}城市在{}没有任何参数需要进行第一轮子站插值".format(city_id, hour))

        #更新
        self.qc_variables = list(set(self.variables) - set(self.need_interpolate_variables))
        print("现在城市{}的质控vars:{}".format(city_id, self.qc_variables))


    def process_potential_site_interpolate(self, city_id, hour, vars):

        # 该城市下可能要进行子站插值的vars
        start_hour = tu.str_datetime_n_hours_before_string(hour, 2)
        site_df = self.site_df[self.site_df['TIMESTAMP'] >= start_hour]
        for var in vars:
            if self.inter_condition.get_is_interpolate_by_var(site_df, var):
                # self.qc_variables = list(set(self.qc_variables) - set([var]))
                self.need_interpolate_variables.append(var)
                self.potential_need_interpolate_variables.append(var)
                logger.warning("s^s^s^智能判断结果: 城市:{} 时间:{} var:{} 需要进行子站插值！".format(city_id, hour, var))
            else:
                logger.warning("s^s^s^智能判断结果: 城市:{} 时间:{} var:{} 不进行子站插值！".format(city_id, hour, var))
                continue

    def execute_train_transmission_by_city(self, city_id, hour):
        '''
        按照训练质控模型 - 传递 - 邻域插值处理的质控
        '''
        #执行质控设备的训练过程  模型存储在self.models里
        self.execute_qc_train_by_city_and_hour(city_id, hour)

        #对质控设备进行质控预测
        self.execute_quality_control_by_city(city_id, hour)
        if self.all_adjust_df[0].empty:
            return

        # 审核一
        # self.execute_model_validity_censor()

        #初始化传递类 计算传递后的预测值
        self.transmitter = Transmitter(self.config, self.models, self.vargroup_qc_version, self.spatial_indexer)
        self.execute_transmission_by_city(hour, city_id)
        if self.all_adjust_df[1].empty:
            return

        #审核二
        self.execute_adj_data_censor()
        if self.all_adjust_df[1].empty:
            return

        #质控数据进行最大值最小值替换之后，写入库中
        self.adjust_df_full = pd.concat([self.all_adjust_df[0].copy(), self.all_adjust_df[1].copy()])
        # self.adjust_df_full = self.set_min_and_max(self.adjust_df_full)
        # self.dao.write_adjust_data(self.adjust_df_full, hour)

        if self.adjust_df_full.empty:
            return

        # #初始化插值类 计算插值后的数据
        self.interpolate = Interpolation(self.config, self.spatial_indexer)
        self.execute_interpolate_by_city(hour, city_id)

        if self.all_adjust_df[3].empty:
            return

        # 插值进行最大值最小值替换后入库
        # self.interpolate_df = self.set_min_and_max(self.interpolate_df)
        # self.dao.write_adjust_data(self.interpolate_df, hour)

        # 处理该城市下的TVOC
        #准备org数据
        self.process_tvoc_direct_org_to_adjust(hour)

    def write_all_adjust_df_to_db(self, hour):
        for i in self.all_adjust_df.keys():
            if not self.all_adjust_df[i].empty:
                if i != 6:
                    self.all_adjust_df[i] = self.set_min_and_max(self.all_adjust_df[i])
                self.dao.write_adjust_data(self.all_adjust_df[i], hour)
                # self.all_adjust_df[i].to_csv('adjust_df_{}.csv'.format(i))

    def format_org_to_adjust(self, org_df, hour, variables):
        '''
        主要适配新上项目的时候，有时候客户要求先看到数，此时直接把org数据展示给客户即可
        '''
        # like_adjust_df = pd.DataFrame(columns=['DEV_ID', 'MEA_POINT_ID', 'ADJ_VALUE', 'VAR', 'VAR_TYPE_ID', 'ADJ_TIME', 'MARK', 'IS_NORMAL'])

        variables_to_format = set(variables + ['TVOC'])
        for var in variables_to_format:
            var_org_df = org_df[['DEV_ID', var]].copy()
            var_org_df.dropna(inplace=True)
            var_dev_list = var_org_df['DEV_ID'].unique()
            var_org_df_index = var_org_df.set_index(['DEV_ID'])
            for dev in var_dev_list:
                measure_point_id = self.dev_measure_point_id_dict[dev][0]
                # adj_value = var_org_df[var_org_df['DEV_ID'] == dev][var].values[0]
                adj_value = var_org_df_index.loc[(dev),var]

                self.all_adjust_df[5] = self.append_df(self.all_adjust_df[5], dev, measure_point_id, adj_value, var, None, hour, 5, 1)
                # like_adjust_df = self.append_df(like_adjust_df, dev, measure_point_id, adj_value, var, None, hour, 3, 1)

        # if not like_adjust_df.empty:
        #     like_adjust_df = self.set_min_and_max(like_adjust_df)
        #     # 写库
        #     self.dao.write_adjust_data(like_adjust_df, hour)

    def drop_columns(self, org_df):
        org_df.drop(['HUMIDITY', 'TEMPERATURE', 'TIMESTAMP', 'SITE_ID', 'COUNT_PM25', 'COUNT_PM10', 'COUNT_SO2',
                     'COUNT_CO', 'COUNT_NO2', 'COUNT_O3', 'COUNT_TVOC', 'COUNT_TSP', 'VARGROUP_ID'], axis=1,
                    inplace=True)

    def set_min_and_max(self, df):
        df['ADJ_VALUE'] = df.apply(lambda row: self.set_min_max_by_var(row), axis=1)
        return df

    def set_min_max_by_var(self, row):
        cur_var = row.VAR
        if row.ADJ_VALUE > self.max_qc_values[cur_var]:
            return self.max_qc_values[cur_var]
        elif row.ADJ_VALUE < self.min_qc_values[cur_var]:
            return self.min_qc_values[cur_var]
        else:
            return row.ADJ_VALUE

    def execute_quality_control_by_city(self, city_id, hour):
        """
        给定某个小时，对给定的城市的质控设备进行质控，model存放在self.models中
        :param city_id: 城市id, list
        :param hour: 某个小时, string
        :return:
        """
        print('进入质控设备自行拟合阶段......')
        #设备MARK
        mark = 0
        # 获取质控设备的当小时的数据
        df = self.prepare_qc_data_by_hour(city_id, hour)
        # logger.info('质控设备当小时有数据的设备：{}'.format(len(device_list)))
        # 进行质控
        for p in self.qc_variables:
            # 获取所有质控设备预测需要的特征
            X = self.get_pred_info_for_pollutant(df, p)
            device_list = X['DEV_ID'].unique()
            i = 0
            for dev in device_list:
                if i%5 == 0 :
                    print('正在处理{}污染物下第{}个质控设备{}'.format(p, i, dev))
                i = i + 1
                # 获取dev_id预测需要的特征  $$$出现了空 没有数据 可能是该设备在这个时间下是没有数据的
                dev_X = X[X['DEV_ID'] == dev].copy()
                if dev_X.empty:
                    # $$$  返回None
                    logger.warning('城市：{} 时间：{} var:{} the device ~~~{} don\'t have org data!'.format(city_id, hour, p, dev))
                    continue
                else:
                    # 判断一下这个设备在这个时刻有没有有效的模型

                    if dev in list(self.models[p].keys()) and self.models[p][dev].get_is_valid():
                        dev_X = self.get_pred_data_one_hour(dev,p,dev_X)
                        if dev_X is not None:
                            pred_val = self.models[p][dev].predict(dev_X)
                            mea_point_id = self.dev_measure_point_id_dict[dev][0]
                            self.all_adjust_df[mark] = self.append_df(self.all_adjust_df[mark], dev, mea_point_id, pred_val, p, None, hour, mark, 1)
                        else:
                            continue
                    else:
                        logger.warning('城市：{} 时间：{} var:{} this qc device ~~~{} don\'t have model! or this model is not valid!'.format(city_id, hour, p, dev))
                        continue

    def execute_transmission_by_city(self, hour, city_id, dev_list=None, is_for_minute=False, org_df=None):
        '''
        给定某个小时，对给定城市的非质控设备进行质控，在调用本函数之前，应该已经调用了train函数

        Args:
            city_id: 城市id, list
            hour: 某个小时, string
        '''
        print("进入传递阶段......")
        #设置MARK
        mark = 1
        if dev_list:
            #获取org数据
            print('进入设备清单回算阶段。。。。')
            df = self.prepare_non_qc_data(hour, city_id, dev_list=dev_list)
            self.models,_ = self.get_models(hour)
            if self.models == None:
                return
            self.transmitter = Transmitter(self.config, self.models, self.vargroup_qc_version, self.spatial_indexer)
        elif org_df is not None:
            print("进入分钟级别质控阶段。。。。")
            df = org_df
            int_hour = tu.str_datatime_to_int_hour_str(hour)
            self.models, int_hour = self.get_models(int_hour, is_for_minute)
            if self.models == None:
                print("没有最近的模型！")
                return
            self.transmitter = Transmitter(self.config, self.models, self.vargroup_qc_version, self.spatial_indexer)
        else:
            df = self.prepare_non_qc_data(hour, city_id=city_id)

        for p in self.qc_variables:
            #获取所有非质控设备预测需要的特征
            if is_for_minute:
                X = self.get_pred_info_for_pollutant(df, p, is_for_minute)
            else:
                X = self.get_pred_info_for_pollutant(df, p)
            X.dropna(inplace=True)
            device_list = X.DEV_ID.unique()
            i = 0
            for dev in device_list:
                if i%100 == 0 :
                    print('正在处理{}污染物下第{}个非质控设备{}'.format(p, i, dev))
                i = i + 1
                # 获取dev_id预测需要的特征  $$$出现了空 没有数据 可能是该设备在这个时间下是没有数据的
                dev_X = X[X['DEV_ID'] == dev].copy()
                if dev_X.empty:
                    #$$$  返回None
                    logger.warning('城市：{} 时间：{} var:{} the device $$${} don\'t have org data! or after clean don\'t have org data!'.format(city_id, hour, p, dev))
                    continue
                else:
                    dev_X.drop(['DEV_ID', 'TIMESTAMP'], axis=1, inplace=True)
                    vargroup_id = df[df['DEV_ID'] == dev]['VARGROUP_ID'].values[0]
                    if is_for_minute:
                        pred_val = self.transmitter.control_for_non_qc_device(dev, vargroup_id, p, dev_X, int_hour,city_id=city_id, dev_list=dev_list,is_for_minute=is_for_minute)
                    else:
                        pred_val = self.transmitter.control_for_non_qc_device(dev, vargroup_id, p, dev_X, hour, city_id=city_id, dev_list=dev_list)
                    mea_point_id = self.dev_measure_point_id_dict[dev][0]
                    if pred_val == None:
                        #$$$
                        logger.warning("城市：{} 时间：{} var:{} 非质控设备$$${}不出质控数据！".format(city_id, hour, p, dev))
                        continue
                    else:
                        self.all_adjust_df[mark] = self.append_df(self.all_adjust_df[mark], dev, mea_point_id, pred_val, p, None, hour, mark, 1)

    def execute_interpolate_by_city(self, hour, city_id, dev_list=None):
        """
        对没有adjust数据的设备进行插值
        dev_list : 需要插值的设备列表
        :return:插值也放到adjust_df 中
        """
        print("进入插值阶段......")
        #设置MARK
        mark = 3
        # 进行插值
        for p in self.qc_variables:
            if dev_list:
                no_data_devices = self.get_no_data_devices_for_back(p, dev_list)
                self.interpolate = Interpolation(self.config, self.spatial_indexer)
            else:
                # 获取p 污染物下没有质控数据的设备编号
                no_data_devices = self.get_no_data_devices(p)
                logger.warning("城市：{} 时间：{} var: {} 没有数据的长度{},没有数据的设备编号^^^{}".format(city_id, hour, p, len(no_data_devices),  no_data_devices))

            # if p == 'PM25':
            #     no_data_df = pd.DataFrame(no_data_devices, columns=['DEV_ID'])
            #     no_data_df.to_csv('pm25_no_data.csv')

            adjust_df_var = self.get_middle(p, self.adjust_df_full)
            i = 0
            for dev in no_data_devices:
                if i%100 == 0:
                    print("正在处理{}污染物下的第{}个没有org数据的{}设备".format(p, i, dev))
                i = i + 1
                interpolate_val = self.interpolate.interpolation_for_qc(city_id, dev, adjust_df_var, p, hour)
                mea_point_id = self.dev_measure_point_id_dict[dev][0]
                if interpolate_val is not None:
                    if interpolate_val == -1:
                        # print('the dev {} don\'t have interpolate_val!'.format(dev))
                        continue
                    else:
                        self.all_adjust_df[mark] = self.append_df(self.all_adjust_df[mark], dev, mea_point_id, interpolate_val, p, None, hour, mark, 1)
                else:
                    logger.warning('城市：{} 时间：{} var：{}the dev ^^^{} don\'t need {} data!'.format(city_id, hour, p, dev, p))
                    continue

    def execute_site_interpolate_by_devices(self, hour, city_id):
        print("进入子站插值补充阶段！")
        logger.info("进入子站插值补充阶段！")
        # 插值前清空中间数据
        self.site_inter.clear_inter_site_df()
        mark = 4
        # 如果子站当小时没有数据，则检查最近三小时是否有数据
        # 如果最近三小时都没有数据，则不进行子站数据审核

        cur_site_df = self.determine_site_df_to_interpolate(city_id)
        if cur_site_df.empty:
            return

        for p in self.qc_variables:
            no_data_devices = self.get_no_data_devices_for_site_interpolate(p)

            if len(no_data_devices) == 0:
                logger.info("城市s^s^s^{}，参数{}没有非质控设备无模型传递且无邻域插值，无需补充子站插值！".format(city_id, p))
            else:
                is_for_var = [p]
                var_df = self.site_inter.execute_site_interpolate(city_id, hour, is_for_var, cur_site_df, mark, dev_list=no_data_devices)
                self.all_adjust_df[mark] = pd.concat([self.all_adjust_df[mark],var_df], axis=0)
            self.site_inter.clear_inter_site_df()
        # if not self.all_adjust_df[mark].empty:
        #     self.all_adjust_df[mark].to_csv('test_site_interpolate.csv')
            # self.dao.write_adjust_data(site_interpolate_df) 

    def execute_site_data_censor(self):
        """
        子站数据进行审核，将异常的子站数据设置成nan
        :return:
        """
        print("进入子站审核阶段......")

        # 如果子站当小时没有数据，则不进行子站数据审核
        if self.site_one_hour_df.empty:
            return
        for p in self.variables:
            if p == 'TVOC':
                continue
            abnormal_site_list = self.qc_censor.site_data_check(self.site_df, p)
            # abnormal_site_list = []
            #去掉当小时的异常数据即可
            self.site_one_hour_df['SITE_' + p] = self.site_one_hour_df.apply(lambda x:np.nan if x.SITE_ID in abnormal_site_list  else x['SITE_'+p], axis=1)
        self.site_df = pd.concat([self.site_before_hours_df, self.site_one_hour_df],axis=0)
        self.site_df.reset_index(drop=True,inplace=True)

    def execute_model_validity_censor(self):
        """
        第一层审核，将异常的质控设备模型设置成FALSE
        :return:
        """
        print("进入审核一阶段......")
        for p in self.qc_variables:
            abnormal_qc_dev_list = self.qc_censor.qc_data_check(self.all_adjust_df[0], p)
            for dev in abnormal_qc_dev_list:
                #先判断该设备是否有模型
                if dev in list(self.models[p].keys()):
                    self.models[p][dev].is_valid = False
                else:
                    continue

    def execute_adj_data_censor(self, dev_list=None):
        """
        审核二，模型传递结束以后审核出经过模型质控后质控数据异常的值
        :return:
        """
        print('进入审核二阶段......')
        for p in self.qc_variables:
            if dev_list is not None:
                transmission_df_var = self.get_middle(p, self.adjust_df_full, is_check=True)
                abnormal_non_qc_dev_list = self.qc_censor.non_qc_data_check(transmission_df_var, p)
                #为了不影响已经入库的adjust数据
                abnormal_non_qc_dev_list = list(set(abnormal_non_qc_dev_list) & set(dev_list))
            else:
                transmission_df_var = self.get_middle(p, self.all_adjust_df[1], is_check=True)
                abnormal_non_qc_dev_list = self.qc_censor.non_qc_data_check(transmission_df_var, p)

            #去掉var下面的异常设备数据待测试
            self.all_adjust_df[1] = self.all_adjust_df[1][~((self.all_adjust_df[1]['DEV_ID'].isin(abnormal_non_qc_dev_list)) & (self.all_adjust_df[1]['VAR'] == p))]

    def execute_qc_train_by_city_and_hour(self, city_id, hour):
        '''
        按城市和小时对质控设备进行训练

        Args:
            city_id: list形式给出城市id
            hour: string, 给出需要出质控模型的小时
        '''
        print('城市{}进入模型训练阶段......'.format(city_id))
        # 1. 准备训练需要的数据
        df = self.prepare_qc_data(city_id, hour)

        # 2. 分参数训练模型
        for p in self.qc_variables:
            # #先判断这个city的这个var需不需要插值 如果插值就直接插值 否则进行训练
            # if self.interpolate_condition.get_is_interpolate_by_var(df, city_id, p):
            #     print("城市:{} var:{} 时间:{}进入子站插值阶段......".format(city_id, p, hour))
            #     self.site_interpolate
            #     continue
            print("开始训练污染物{}".format(p))
            logger.debug('before cleaned device num is {}'.format(len(df.DEV_ID.unique())))
            # 判断场景
            self.scenario = self.judgment_scene(df, hour, p)
            #$$$ 有些污染物是不出数的
            y, X = self.get_train_info_for_pollutant(df, p, hour, self.scenario)
            # print("X:{}".format(X.columns))
            if y.empty or X.empty:
                logger.debug('{} don\'t have train data!'.format(p))
                continue
            else:
                logger.debug('train data after cleaned device num is {}'.format(len(X.DEV_ID.unique())))
                self.model_dispatcher.execute_qc_models(p, y, X, hour, self.scenario, city_id)

        self.models = self.model_dispatcher.get_models()

    def get_no_data_devices(self, var):
        """
        获取该=应该出var的质控数据但是因为没有org而没有出质控数据的设备编号
        :param var:
        :return:
        """
        if not var in self.dic_city.keys():
            return []
        #获取没有数据的dev_id
        var_devices = self.adjust_df_full[self.adjust_df_full['VAR'] == var]['DEV_ID'].unique().tolist()
        temp_df = self.device_list_info[~self.device_list_info['DEV_ID'].isin(var_devices)]
        #再从没有数据的dev中留下出var数据的dev
        dev_list = self.dic_city[var]
        no_data_devices_df = temp_df[(temp_df['DEV_ID'].isin(dev_list)) & (temp_df['RELATE_SITE_ID'] == -1)]
        self.no_data_devices[var] = no_data_devices_df['DEV_ID'].unique().tolist()
        return self.no_data_devices[var]

    def get_no_data_devices_for_site_interpolate(self, var):
        #var下邻域插值后有数据的设备列表
        # print('@@@@@@@@@@@var{}获取没有数据的设备编号'.format(var))
        interpolate_dev_df = self.all_adjust_df[3][self.all_adjust_df[3]['VAR'] == var].copy()
        if not interpolate_dev_df.empty:
            interpolate_dev_list = interpolate_dev_df['DEV_ID'].unique().tolist()
        else:
            interpolate_dev_list = []
        #邻域插值后还没有的设备列表
        devices_for_site_interpolate = set(self.no_data_devices[var]).difference(set(interpolate_dev_list))
        return devices_for_site_interpolate

    def get_no_data_devices_for_back(self, var, dev_list):
        if not var in self.dic_city.keys():
            return []
        # 获取没有数据的dev_id
        var_devices = self.all_adjust_df[1][self.all_adjust_df[1]['VAR'] == var]['DEV_ID'].unique().tolist()
        no_data_dev_list = list(set(dev_list) - set(var_devices))
        #再从没有数据的dev中留下出var数据的dev
        dev_list = self.dic_city[var]
        no_data_devices = list(set(no_data_dev_list) & set(dev_list))
        return no_data_devices

    def prepare_qc_data(self, city_id, hour):
        '''
        准备质控所需要的数据
        '''
        # 1. 分别查询出设备数据和子站数据并且进行merge
        qc_dev_df = self.dao.query_qc_dev_org_data_by_city_month(city_id, hour)
        dev_site_df = qc_dev_df.merge(self.site_df, on=['TIMESTAMP', 'SITE_ID'], how='left')
        return dev_site_df

    def prepare_non_qc_data(self, hour, city_id, dev_list=None):
        '''
        准备非质控设备当小时的数据
        '''
        if dev_list is not None:
            #如果设备列表不为None，按设备清单准备数据
            self.non_qc_dev_df = self.dao.query_org_data_by_device_list(dev_list, hour, hour)
            return self.non_qc_dev_df
        else:
            # 如果设备列表为None，按城市获取数据
            self.non_qc_dev_df = self.dao.query_non_qc_dev_org_data_by_city(city_id, hour)
            return self.non_qc_dev_df

    def prepare_qc_data_by_hour(self, city_id, hour):
        """
        准备质控设备当小时的数据
        :param city_id:
        :param hour:
        :return:
        """
        self.qc_dev_df = self.dao.query_qc_dev_org_data_by_city(city_id, hour)
        return self.qc_dev_df

    def get_train_info_for_pollutant(self, df, p_name, hour, scenario):
        df = self.get_cleaned_data(df, p_name, scenario)

        df = df[~df['DEV_ID'].isin(self.qc_device_tabu_dict[p_name])]
        df = self.get_properly_sampled_data(df, p_name, hour)

        #$$$
        if df.empty:
            logger.debug('The train data dataframe is empty after clearn or the dataframe have not {} at {}! '.format(p_name, hour))
            return pd.DataFrame(),pd.DataFrame()
        else:
            y, X = self.feature_gen.get_prepared_data_for_train(df, p_name)
            return y, X

    def get_pred_info_for_pollutant(self, df, p_name, is_for_minute=False):
        if is_for_minute:
            df = self.get_cleaned_data(df, p_name, is_for_train=False, is_for_minute=is_for_minute)
        else:
            df = self.get_cleaned_data(df, p_name, is_for_train=False)
        df = df[~df['DEV_ID'].isin(self.nonqc_device_transmission_tabu_dict[p_name])]
        #$$$ 做不做容错？
        X = self.feature_gen.get_prepared_data_for_prediction(df, p_name)
        return X

    def get_properly_sampled_data(self, df, p_name, hour):

        # df.to_csv('before_sample_{}.csv'.format(p_name))

        df = self.org_s.cur_hour_oversampler(df, p_name, hour)
        df = self.org_s.high_humidity_undersampler(df, p_name, hour)
        # df.to_csv('after_sample_{}.csv'.format(p_name))

        return df

    def get_cleaned_data(self, df, p_name, scenario=None, is_for_train=True, is_for_minute=False):
        '''
        利用OrgFilter将数据处理成符合要求的数据, print的地方都需要处理成error log

        Args:
            df: 全参数的org数据，如果是质控设备，则应整合了site数据
            p_name: 要处理的污染物/参数名称
            is_for_train: 是否为训练数据

        Return:
            df: clean之后的数据
        '''
        #
        tmp_df = df.copy()
        tmp_df['TIMESTAMP'] = tmp_df['TIMESTAMP'].astype(datetime.datetime)
        tmp_df['TIMESTAMP'] = tmp_df.apply(lambda x: tu.datetime_to_string(x.TIMESTAMP) if type(x.TIMESTAMP) is not type('a') else x.TIMESTAMP, axis=1)
        # $$$
        if tmp_df.empty:
            print('The dataframe is empty')
            return df

        l1_list = tmp_df['DEV_ID'].unique().tolist()
        if is_for_minute==False:
            var_df = self.org_f.few_entry_filter(tmp_df, p_name)
        else:
            var_df = tmp_df
            print('不用判断分钟级别的条数！')
        l2_list = var_df['DEV_ID'].unique().tolist()
        logger.debug('每小时capture数据条数不足的设备ID清单：{}'.format(set(l1_list)-set(l2_list)))
        if var_df.empty:
            logger.info('The dataframe is empty after {}'.format('few_entry_filter'))
            return var_df

        var_df = self.org_f.column_filter(var_df, p_name, is_for_train)
        if var_df.empty:
            return var_df

        var_df = self.org_f.nan_filter(var_df)
        l3_list = var_df['DEV_ID'].unique().tolist()
        logger.debug('Nan值的设备ID清单：{}'.format(set(l2_list) - set(l3_list)))

        if var_df.empty:
            logger.info('The dataframe is empty after {}'.format('nan_filter'))
            return var_df

        if is_for_train:
            var_df = self.org_f.not_enough_device_entry_filter(var_df)
            l4_list = var_df['DEV_ID'].unique().tolist()
            logger.debug('训练数据条数不够的设备ID清单：{}'.format(set(l3_list) - set(l4_list)))
            if var_df.empty:
                logger.info('The dataframe is empty after {}'.format('not_enough_device_entry'))
                return var_df

            var_df = self.org_f.var_specific_filter(var_df, p_name, scenario)
            l5_list = var_df['DEV_ID'].unique().tolist()
            logger.debug('特殊通道（eg.PM10）处理掉的设备ID清单：{}'.format(set(l4_list) - set(l5_list)))
            if var_df.empty:
                logger.info('The dataframe is empty after {}'.format('var_specific_filter'))
                return var_df

        return var_df

    def judgment_scene(self, df, hour, var):
        # 在特征处理之前，判断场景类型
        if var=='PM10':
            df['SITE_PM10/SITE_PM25'] = df.apply(lambda x: x.SITE_PM10/x.SITE_PM25,axis=1)
            df['PM10/PM25'] = df.apply(lambda x: x.PM10/x.PM25, axis=1)
            self.scene = self.scenario_agent.determine_scenario(
                    df, var, hour)
            logger.info('judgment_scene:{}'.format(self.scene))
            return self.scene

    def get_pred_data_one_hour(self, dev, p, dev_X):
        # 由于扬尘天气和普通的时候输入的特征不同

        if p == 'PM10' and ('lr' in self.models[p][dev].model_name):
            dev_X.drop(['DEV_ID', 'TIMESTAMP', 'PM25^1'], axis=1, inplace=True)
            return dev_X
        else:
            dev_X.drop(['DEV_ID', 'TIMESTAMP'], axis=1, inplace=True)
            return dev_X

    def df_dict(self):
        df_temp = self.device_list_info[['DEV_ID','MEASURE_POINT_ID']].copy()
        return df_temp.set_index('DEV_ID').T.to_dict('list')

    def append_df(self, df, dev_id, mea_point_id, adj_value, var, var_type_id, adj_time, mark, is_normal):
        adjust_dict = {'DEV_ID':dev_id, 'MEA_POINT_ID':mea_point_id, 'ADJ_VALUE':adj_value, 'VAR':var, 'VAR_TYPE_ID':var_type_id, 'ADJ_TIME':adj_time, 'MARK':mark, 'IS_NORMAL':is_normal}
        return df.append(adjust_dict, ignore_index=True)


    def get_middle(self, p, df, is_check=False):
        # 每个var级别的质控数据进行排序
        df_var = df[df['VAR'] == p].copy()
        df_var.sort_values(by=['ADJ_VALUE'], inplace=True)
        N = len(df_var)
        n = int(self.tabu_percent[p]*N)
        if is_check:
            #做审核的时候只去掉高值
            return df_var[0:N - n]
        else:
            #做插值的时候 高低值都要去掉
            return df_var[n:N - n]

    def get_models(self, hour, is_for_minute=False):
        pre_str_dict = {}
        directory = self.get_dir(hour)
        if os.path.exists(directory):
            model_list = os.listdir(directory)
        else:
            if is_for_minute:
                hour = tu.str_datetime_to_int_hour_minus_one_hour(hour)
                hour = tu.datetime_to_string(hour)
                directory = self.get_dir(hour)
                if os.path.exists(directory):
                    model_list = os.listdir(directory)
                else:
                    return None, hour
            else:
                return None, hour
        for model in model_list:
            set_str = model.split('_')
            pre_str_dict['{}_{}_{}'.format(set_str[0], set_str[1], set_str[2])] = model
        return pre_str_dict, hour

    def get_dir(self, hour):
        model_folder_date = fu.get_model_folder_name_by_date(hour)
        model_folder_hour = fu.get_model_folder_name_by_hour(hour)
        directory = "{}{}/{}".format(self.directory, model_folder_date, model_folder_hour)
        return directory

    def concat_adjust_df(self):
        all_adjust_df_temp = pd.DataFrame()
        for key in self.all_adjust_df.keys():
            all_adjust_df_temp = pd.concat([all_adjust_df_temp, self.all_adjust_df[key].copy()])
        return all_adjust_df_temp
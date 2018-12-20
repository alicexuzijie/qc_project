# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sys
import common
import dao.sql_utility as su
import dao.sql_api as sql_api
import utility.mysql_connector as mc
import utility.time_utility as tu
import utility.utility as utility
import config.qc_config as qc
from error_demo.error_code import *
from log import log
from multiprocessing.managers import BaseManager

logger = log.log_demo()


class DataOperationsByMysql(sql_api.DataOperations):
    def __init__(self, config, hour):
        super().__init__()
        self.section = 'MYSQL-SENSOR1A'
        self.config = config
        self.n_days_train_data = self.config.get_config_global_data('num_days_for_training')
        self.n_days_train_data = int(self.n_days_train_data)
        start_hour = tu.datetime_n_days_before_string(tu.time_str_to_datetime(hour), self.n_days_train_data)
        self.active_devices_info = self.query_all_active_devices_info(hour)
        abbr_df = self.query_abbr()
        self.init_workable_vars(abbr_df)
        self.air_quality_data = self.query_site_data_by_time_interval(start_hour, hour)

    def query_all_active_devices_info(self, hour):
        # 获取sql语句
        sql_statement = su.compose_sensor_query_statement(hour)
        # print(sql_statement)
        # 获取所有的设备编号 （ DEV_ID,SEN_DEV_TYPE_ID,RELATE_SITE_ID,
        # VARGROUP_ID,LATITUDE,LONGITUDE,CITYID）
        active_devices_df = mc.mysql_export_data_to_df(sql_statement,self.section)
        if active_devices_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif active_devices_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        active_devices_df = su.strip_column(active_devices_df, 'RELATE_SITE_ID')
        active_devices_df['RELATE_SITE_ID'] = active_devices_df['RELATE_SITE_ID'].astype(int)
        return active_devices_df

    def query_active_devices(self):
        sensor_df = self.active_devices_info.drop(['ABBR_CODE', 'LATITUDE', 'LONGITUDE', 'CITYID',  'ALTITUDE'], axis=1)
        if sensor_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif sensor_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        return sensor_df

    def query_active_devices_by_city(self, city_list):
        # 根据城市id，获取某个城市的设备编号信息
        sensor_df = self.active_devices_info[
                self.active_devices_info['CITYID'].isin(city_list)]
        sensor_df = sensor_df.drop(['ABBR_CODE', 'LATITUDE', 'LONGITUDE', 'CITYID', 'ALTITUDE'], axis=1)
        sensor_df = sensor_df.reset_index(drop=True)
        if sensor_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif sensor_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')

        return sensor_df

    def query_active_devices_by_device_list(self, device_list):
        # 根据设备编号获取某些设备的信息
        sensor_df = self.active_devices_info[
                self.active_devices_info['DEV_ID'].isin(device_list)]
        sensor_df = sensor_df.drop(['ABBR_CODE', 'LATITUDE', 'LONGITUDE', 'CITYID',  'ALTITUDE'], axis=1)
        sensor_df = sensor_df.reset_index(drop=True)
        if sensor_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif sensor_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        return sensor_df

    def query_channels(self):
        # 根据VARGROUP_ID，获取出数通道
        sql_statement = su.compose_transducerlist_query_statement()
        transducerlist_df = mc.mysql_export_data_to_df(sql_statement, self.section)
        if transducerlist_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif transducerlist_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        transducerlist_df = transducerlist_df.dropna().reset_index(drop=True)
        return transducerlist_df

    def query_qualitycontrol_version(self):
        # 获取质控版本
        sql_statement = su.compose_version_query_statement()
        version_df = mc.mysql_export_data_to_df(sql_statement, self.section)
        if version_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif version_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        return version_df

    def query_field_name(self):
        # 获取capture表列名字段
        # 获取需要的字段，并传递给函数compose_field_name_statement，在sql中会过滤不需要的字段
        field_tuple = utility.list_to_tuple(self.field_name_full_set)
        sql_statement = su.compose_field_name_statement(field_tuple=field_tuple)
        field_name_df = mc.mysql_export_data_to_df(sql_statement, self.section)
        if field_name_df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif field_name_df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        field_name_df.set_index("DEV_TYPE_ID", inplace=True)
        return field_name_df

    def query_consistency_model(self):
        # 获取所有在线设备一致性模型
        sql_statement = su.compose_consistency_model_query_statement()
        consistency_model = mc.mysql_export_data_to_df(
            sql_statement, self.section)
        return consistency_model

    def query_field_names_by_capture_x(self, device_info_df):
        # 根据'SEN_DEV_TYPE_ID',构建字典{"1":[],"3":[],"7":[]......}
        # 为了区分设备编号该在哪个capture_x取分钟原始数据
        capture_x_and_device_list = {}
        for sen_dev_type_id in pd.unique(device_info_df['SEN_DEV_TYPE_ID']):
            device_list = device_info_df.loc[device_info_df['SEN_DEV_TYPE_ID'] == sen_dev_type_id]
            capture_x_and_device_list[sen_dev_type_id] = list(device_list['DEV_ID'])
        return capture_x_and_device_list

    def query_capture_data_by_capture_x(
            self, starttime, endtime, capture_x, field_name, device_list):
        # 根据hour(时间), capture_x(哪个capture表), field_name(要获取的字段),
        # device_list(设备编号)，获取到分钟原始数据
        #  compose_capture_data_query_statement 拼接sql
        sql_statement = su.compose_capture_data_query_statement(
            field_name=field_name,
            sen_dev_type_id=capture_x,
            device_list=device_list,
            starttime=starttime,
            endtime=endtime
        )
        capture_data = mc.mysql_export_data_to_df(sql_statement, self.section)
        if capture_data is None:
            logger.info('capture_%s不存在', capture_x)
        else:
            logger.info('capture_%s的数据长度为%s', capture_x, len(capture_data))
        return capture_data

    def query_capture_data_cross_hour(self, starttime, endtime, device_info_df):
        # 根据hour(时间)、device_info_df(设备信息)，
        # 获取分钟级别的原始数据，并返回字典{'1'df:,'3':df...}
        try:
            # 先获取不同的capture_x,不同的字段
            field_name_df = self.query_field_name()
            dic_capture_data = {}
            # 根据'SEN_DEV_TYPE_ID'区分设备编号
            capture_and_device = self.query_field_names_by_capture_x(
                device_info_df)
            # field_name_df与capture_and_device.keys()取交集，因为表信息不全
            capture_type = list(set(field_name_df.index).intersection(
                set(capture_and_device.keys())))
            # 循环每个capture_x，并取数据,最后修改列名
            for capture_x in capture_type:
                capture_data = self.query_capture_data_by_capture_x(
                    starttime, endtime, capture_x, field_name_df.ix[capture_x, "COL_NAME"], capture_and_device[capture_x])            
                if capture_data is not None:
                    # 增加VARGROUP_ID字段
                    capture_data = su.append_device_info_to_df(
                        capture_data, device_info_df, ['VARGROUP_ID'])
                    if capture_x in [1, 7]:
                        capture_data.rename(
                            columns={'OUTSIDE_HUMIDITY': 'HUMIDITY', 'OUTSIDE_TEMPERATURE': 'TEMPERATURE'},
                            inplace=True)
                    elif capture_x in [3]:
                        capture_data.rename(
                            columns={'OUTSIDE_HUMIDITY_1': 'HUMIDITY', 'OUTSIDE_TEMPERATURE_1': 'TEMPERATURE'},
                            inplace=True)
                    dic_capture_data[capture_x] = capture_data
                    # print(dic_capture_data)
            if len(dic_capture_data) == 0:
                raise NoneDfError('capture_data为空，可能数据库连接有异常或者数据库没数据')
            return dic_capture_data
        except BaseError as e:
            e.setdata({'key': 'DataOperationsByMysql.n_days_train_data'})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise CaptureValueError('原始数据有问题')

    def query_capture_data_by_hour(self, hour, device_info_df):
        # 获取起始时间,例如2018-12-12 19：00：00 至 2018-12-12 19：59：59
        starttime = tu.datetime_n_hours_before_string(
            tu.time_str_to_datetime(hour), 1)
        endtime = starttime[0:13] + ':59:59'
        cap_data = self.query_capture_data_cross_hour(starttime, endtime, device_info_df)
        return cap_data

    def query_capture_data_by_minute(self, hour, device_info_df):
        # 获取起始时间,例如2018-12-12 19：00：00 至 2018-12-12 19：17：30
        starttime = tu.str_datetime_n_seconds_before_string(hour, 1050)
        cap_data = self.query_capture_data_cross_hour(starttime, hour, device_info_df)
        return cap_data

    def query_org_data_by_org_x(self, org_x, device_list, endtime, starttime=None):
        # org_x（1，2，35，6，7）, device_list（设备编号）, starttime, endtime
        # 根据org_x, device_list, starttime, endtime获取org数据
        sql_statement = su.compose_org_data_query_statement(
            sen_dev_type_id=org_x,
            device_list=device_list,
            starttime=starttime,
            endtime=endtime
        )
        org_data = mc.mysql_export_data_to_df(sql_statement, self.section)
        if org_data is None:
            logger.info('org_%s不存在', org_x)
        else:
            logger.info('org_%s的数据长度为%s', org_x, len(org_data))
        return org_data

    def merge_org_data_by_month(self, org_x, device_list, endtime, starttime=None):
        # 跨月取数时，对不同月份的数据进行合并,同一个org_x
        # 判断是否时同一个月的数据
        if starttime is None or starttime[5:7] == endtime[5:7]:
            org_data = self.query_org_data_by_org_x(
                org_x, device_list, endtime, starttime)
            return org_data
        else:
            # 获取上月的月末时间
            month_endtime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.last_day_of_a_month(
                        tu.time_str_to_datetime(starttime))[0]))
            # 获取当月的月初时间
            month_starttime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.first_day_of_a_month(
                        tu.time_str_to_datetime(endtime))[0]))
            # 获取上个月的org数据
            org_data_last_month = self.query_org_data_by_org_x(
                org_x, device_list, month_endtime, starttime)
            # 获取当月的org数据
            org_data_this_month = self.query_org_data_by_org_x(
                org_x, device_list, endtime, month_starttime)
            # 合并org数据
            org_data = org_data_last_month.append(org_data_this_month)
            return org_data

    def query_org_data_for_device_info_df(self, endtime, device_info_df, starttime=None):
        # 获取不同org_x的org数据，并合并
        try:
            finall_org_data = pd.DataFrame()
            # 区分不同org的设备编号，返回字典{'1':[],'3':[]...}
            org_x_and_device_list = self.query_field_names_by_capture_x(device_info_df)
            # 对不同的org进行遍历，分别获取org数据
            for org_x in org_x_and_device_list:
                # print(org_x)
                if org_x in [1, 3, 7]:
                    # 获取数据
                    org_data = self.merge_org_data_by_month(
                        org_x,
                        list(org_x_and_device_list[org_x]),
                        endtime,
                        starttime)
                    # 增加字段
                    org_data = su.append_device_info_to_df(
                        org_data, device_info_df,
                        ['VARGROUP_ID', 'RELATE_SITE_ID'])
                    # 对一些列重命名，并增加一些列（convert_dic_format_for_org），最后合并数据
                    finall_org_data = finall_org_data.append(
                        su.convert_dic_format_for_org(org_data))
                    # 重置索引
                    finall_org_data = finall_org_data.reset_index(drop=True)
                    finall_org_data['COUNT_PM25'] = finall_org_data['COUNT_PM25'].astype('float64')
                    # logger.info(finall_org_data)
            if finall_org_data.empty:
                raise NoneDfError('org_data为空，可能数据库连接有异常或者数据库没数据')
            return finall_org_data
        except BaseError as e:
            raise OrgValueError('ORG数据有问题')

    # 质控设备7天数据
    def query_qc_dev_org_data_by_city_month(self, city_id, hour):
        # 获取某个城市质控设备的7天数据
        try:
            # 获取城市的设备信息
            device_info_df = self.query_active_devices_by_city(city_id)
            device_info_df['RELATE_SITE_ID'] = device_info_df.apply(lambda x: int(x.RELATE_SITE_ID), axis=1)
            # device_info_df['RELATE_SITE_ID'] = device_info_df['RELATE_SITE_ID'].astype('int64')
            # 获取质控设备信息
            device_info_df = device_info_df[device_info_df['RELATE_SITE_ID'] > 0]
            # 获取hour7天之前的时间点
            starttime = tu.datetime_n_days_before_string(
                    tu.time_str_to_datetime(hour), self.n_days_train_data)
            # 获取org数据
            finall_org_data = self.query_org_data_for_device_info_df(
                hour, device_info_df, starttime)
            return finall_org_data
        except BaseError as e:
            e.setdata({'key': 'query_qc_dev_org_data_by_city_month', 'hour': hour,
                       'city_id': city_id})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise OrgValueError('ORG数据有问题')

    # 非质控设备1小时数据
    def query_non_qc_dev_org_data_by_city(self, city_id, hour):
        try:
            # 获取设备编号
            device_info_df = self.query_active_devices_by_city(city_id)
            # 获取非质控设备编号
            device_info_df['RELATE_SITE_ID'] = device_info_df['RELATE_SITE_ID'].astype('int64')
            device_info_df = device_info_df[device_info_df['RELATE_SITE_ID'] == -1]
            # 获取org数据
            finall_org_data = self.query_org_data_for_device_info_df(
                hour, device_info_df)
            return finall_org_data
        except BaseError as e:
            e.setdata({'key': 'query_non_qc_dev_org_data_by_city_month', 'hour': hour,
                       'city_id': city_id})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise OrgValueError('ORG数据有问题')

    # 根据设备编号获取org数据
    def query_org_data_by_device_list(self, device_list, start_time, end_time):
        """
        获取org数据
        device_list：设备列表
        start_time：开始时间
        end_time：结束时间
        """
        try:
            # 获取设备编号
            device_info_df = self.query_active_devices_by_device_list(device_list)
            # 获取非质控设备编号
            device_info_df = device_info_df[device_info_df['DEV_ID'].isin(device_list)]
            # 获取org数据
            finall_org_data = self.query_org_data_for_device_info_df(
                end_time, device_info_df, start_time)
            return finall_org_data
        except BaseError as e:
            e.setdata({'key': 'query_org_data_by_device_list', 'hour': start_time,
                       'device_list': device_list})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise OrgValueError('ORG数据有问题')

    # 质控设备1小时数据
    def query_qc_dev_org_data_by_city(self, city_id, hour):
        try:
            # 获取设备编号
            device_info_df = self.query_active_devices_by_city(city_id)
            # 获取质控设备编号
            device_info_df['RELATE_SITE_ID'] = device_info_df['RELATE_SITE_ID'].astype('int64')
            device_info_df = device_info_df[device_info_df['RELATE_SITE_ID'] > 0]
            # 获取org数据
            finall_org_data = self.query_org_data_for_device_info_df(
                hour, device_info_df)
            return finall_org_data
        except BaseError as e:
            e.setdata({'key': 'query_qc_dev_org_data_by_city', 'hour': hour,
                       'city_id': city_id})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise OrgValueError('ORG数据有问题')

    def query_site_data_by_time_interval(self, start_hour, end_hour, city_id=-1):
        """
        # 子站数据
        """
        # 获取设备信息
        if city_id == -1:
            device_info_df = self.active_devices_info
        else:
            # print(self.active_devices_info.info())
            device_info_df = self.active_devices_info[self.active_devices_info['CITYID'].isin(city_id)]
        # 获取子站id
        device_info_df['RELATE_SITE_ID'] = device_info_df.apply(lambda x: int(x.RELATE_SITE_ID), axis=1)
        site_id = device_info_df[device_info_df['RELATE_SITE_ID'] > 0]['RELATE_SITE_ID'].values
        site_data = self.query_and_proc_cross_month_site_data(site_id, start_hour, end_hour)
        return site_data

    def query_site_train_data_by_city(self, cityid, hour, is_cache=False):
        """
        # 按照配置的训练时间长度获取子站数据，训练时间长度 = self.n_days_train_data

        Args:
            cityid: 需要查询子站数据的城市id列表
            hour: 目标小时，查询时按照self.n_days_train_data的配置往前推n天
            is_cache: 是否启动缓存，如果启动缓存，则先查询全国数据，然后存储到内存中下次使用
        """
        start_hour = tu.datetime_n_days_before_string(tu.time_str_to_datetime(hour), self.n_days_train_data)
        end_hour = hour

        if is_cache == False:
            return self.query_site_data_by_time_interval(start_hour, end_hour, cityid)
        else:
            if self.air_quality_data.empty:
                self.air_quality_data = self.query_site_data_by_time_interval(start_hour, end_hour)

            # 先获取城市子站id
            device_info_df = self.active_devices_info[
                self.active_devices_info['CITYID'].isin(cityid)]
            device_info_df['RELATE_SITE_ID'] = device_info_df['RELATE_SITE_ID'].astype('int64')
            site_id = device_info_df[device_info_df['RELATE_SITE_ID'] > 0]['RELATE_SITE_ID'].values
            # 从已经缓存的self.air_quality_data中匹配数据
            site_data = self.air_quality_data[
                self.air_quality_data['SITE_ID'].isin(site_id)]
            return site_data

    def query_and_proc_cross_month_site_data(self, site_id_list, starttime, hour):
        try:
            # 获取子站数据
            site_id_list = utility.list_to_tuple(site_id_list)
            site_data = self.query_cross_month_site_data(site_id_list, starttime, hour)
            return site_data
        except BaseError as e:
            raise SiteValueError('子站数据有问题')

    def query_site_data_within_one_month(self, site_id_list, starttime, endtime):
        # 根据site_id, starttime, endtime 获取子站数据
        sql_statement = su.compose_site_query_statement(
            site_id_list, starttime, endtime)
        site_data = mc.mysql_export_data_to_df(sql_statement, self.section)
        site_data = site_data.replace([None], np.nan)
        return site_data

    def query_cross_month_site_data(self, site_id_list, starttime, endtime):
        if starttime[5:7] == endtime[5:7]:
            site_data = self.query_site_data_within_one_month(site_id_list, starttime, endtime)
            return site_data
        else:
            month_endtime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.last_day_of_a_month(
                        tu.time_str_to_datetime(starttime))[0]))
            month_starttime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.first_day_of_a_month(
                        tu.time_str_to_datetime(endtime))[0]))
            # 获取上个月子站数据
            site_data_last_month = self.query_site_data_within_one_month(
                site_id_list, starttime, month_endtime)
            # 获取当月子站数据
            site_data_this_month = self.query_site_data_within_one_month(
                site_id_list, month_starttime, endtime)
            # 数据合并
            # 数据合并
            if site_data_last_month is not None:
                site_data = site_data_last_month.append(site_data_this_month)
            else:
                site_data = site_data_this_month
        if site_data is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif site_data.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        return site_data

    def query_aq_type_in_dict(self):
        # 获取污染物类型对应的VAR_TYPE_ID
        sql_statement = su.compose_aq_type()
        df = mc.mysql_export_data_to_df(sql_statement, self.section)
        if df is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif df.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        my_dict = utility.two_column_df_to_dict(df, 'VAR_TYPE_ID', 'VAR_NAME')
        my_dict[1] = utility.exclude_dot_in_var_name(my_dict[1])
        return my_dict

    def query_devices_latitude_longitude(self, relate_site_id=None, device_list=None, city_id=None):
        # 获取设备的经纬度、高度
        sensor_info = self.active_devices_info.copy()
        # 获取某些设备的经纬度、高度
        if device_list is not None:
            sensor_info = sensor_info[sensor_info['DEV_ID'].isin(device_list)]
        # 获取某个城市的设备的经纬度、高度
        if city_id is not None:
            sensor_info = sensor_info[sensor_info['CITYID'].isin(city_id)]
        # 获取质控或者非质控设备的经纬度、高度
        if relate_site_id == 1:
            sensor_info = sensor_info[sensor_info['RELATE_SITE_ID'] > 0]
        elif relate_site_id == -1:
            sensor_info = sensor_info[sensor_info['RELATE_SITE_ID'] == -1]
        if sensor_info is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif sensor_info.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        # 修改列名
        sensor_info.rename(
            columns={'LONGITUDE': 'GOOGLELONGITUDE',
                     'LATITUDE': 'GOOGLELATITUDE'},
            inplace=True)
        # 去掉空格？
        sensor_info['RELATE_SITE_ID'] = sensor_info['RELATE_SITE_ID'].astype(str)
        sensor_info = su.strip_column(sensor_info, 'RELATE_SITE_ID')
        # 修改列的数据类型
        sensor_info['RELATE_SITE_ID'] = sensor_info['RELATE_SITE_ID'].astype(int)
        sensor_info['GOOGLELONGITUDE'] = sensor_info['GOOGLELONGITUDE'].astype(float)
        sensor_info['GOOGLELATITUDE'] = sensor_info['GOOGLELATITUDE'].astype(float)
        # 重置索引
        sensor_info = sensor_info.reset_index(drop=True)
        return sensor_info

    def query_site_latitude_longitude(self, city_id=None, site_id=None):
        # 获取一个城市的子站经纬度
        sql = su.compose_site_id_query_statement(city_id, site_id)
        site_info = mc.mysql_export_data_to_df(sql, self.section)
        if site_info is None:
            raise NoneDfError('未返回df数据，可能数据库连接有异常')
        elif site_info.empty:
            raise EmptyDfError('查询数据库获取的df数据为空')
        return site_info

    def write_org_db(self, org_dic, hour):
        # org 数据存储
        try:
            year_month = self.org_data_frame_drop_column(org_dic, hour)
            for key in org_dic.keys():
                if key in [1]:
                    self.write_org_db_by_x(2, org_dic[key], year_month)
                elif key in [3, 7]:
                    self.write_org_db_by_x(key, org_dic[key], year_month)
                else:
                    raise InnerParameterError('org_dic中的key不在规定的有效范围')
        except BaseError as e:
            e.setdata({'key': 'write_org_db', 'org_dic': org_dic})
            logger.error('code:%s,name:%s,message:%s,data:%s',
                         e.code, e.name,
                         e.message, e.getdata(), exc_info=True)
            raise OrgValueError('ORG数据有问题')

    def write_org_db_by_x(self, org_x, org_data, year_month):
        # 表名拼接
        target_table_name = 'DEV_ORG_PER_HOUR_' + str(org_x) + '_' + year_month
        # print(target_table_name)
        mc.mysql_import_data_from_df_batch(
            org_data,
            target_table_name,
            self.section,
            replace=True,
            verbose=True)

    def org_data_frame_drop_column(self, org_dic, hour):
        # 根据org_x修改列名，并获取时间
        for key in org_dic.keys():
            org_dic[key] = org_dic[key].drop(['COUNT_HUMIDITY', 'COUNT_TEMPERATURE'], axis=1)
            org_dic[key].rename(columns={'HUMIDITY': 'OUTSIDE_HUMIDITY', 'TEMPERATURE': 'OUTSIDE_TEMPERATURE', 'MEA_POINT_ID': 'MEASURE_POINT_ID'},
                                inplace=True)
            org_dic[key].index = range(len(org_dic[key]))
        year_month = str(hour[:4]) + str(hour[5:7])
        return year_month

    def convert_adjust_df(self, adjust_data):
        #adjust_data.to_csv('adjust_data.csv')
        var_info_dict = {'PM25': 1, 'PM10': 2, 'SO2': 3,
                         'CO': 4, 'NO2': 5, 'O3': 6, 'AQI': 7,
                         'TVOC': 8, 'NO': 9, 'TSP': 10, }
        for key in var_info_dict:
            data_type = utility.get_qa_type_names_by_ids([key], var_info_dict)
            adjust_data['VAR_TYPE_ID'][adjust_data.VAR == key] = data_type
        adjust_data.drop(['VAR'], axis=1, inplace=True)
        adjust_data =adjust_data.reset_index(drop=True)
        return adjust_data

    def write_adjust_data(self, ad_data, adj_time):
        #ad_data.to_csv('adjust_data.csv')
        if ad_data is not None:
            adjust_data = ad_data.copy()
            # 质控数据，var_type_id 转换为 污染物名称
            adjust_data = self.convert_adjust_df(adjust_data)
            # 表名拼接
            target_table_name = 'DEVICE_ADJUST_VALUE_BYHOUR_' + adj_time[:4] + adj_time[5:7]
            # 删除分月表中重复的数据
            self.delete_adjust_data(target_table_name, adjust_data, adj_time)
            # 数据存储
            mc.mysql_import_data_from_df_batch(
                adjust_data, target_table_name, self.section, replace=True)
            # 删除总表中重复数据
            self.delete_adjust_data('DEVICE_ADJUST_VALUE_BYHOUR',
                                    adjust_data, adj_time)
            adjust_data.drop(['IS_NORMAL'], axis=1, inplace=True)
            mc.mysql_import_data_from_df_batch(
                adjust_data, 'DEVICE_ADJUST_VALUE_BYHOUR',
                self.section, replace=True)
        else:
            print('质控数据为空')
            pass

    def delete_adjust_data(self, table, ad_data, adj_time):
        if ad_data is not None:
            var_type_id_list = pd.unique(ad_data['VAR_TYPE_ID'])
            for i in var_type_id_list:
                dev_list = list(ad_data[ad_data.VAR_TYPE_ID==i].DEV_ID)
                where_clause = su.compose_delete_adjust_data_query_statement(dev_list, adj_time, [int(i)])
                # print(where_clause)
                mc.mysql_delete_table(
                    table, self.section, where_clause, verbose=False)
        else:
            print('无数据需要删除')

    def query_abbr(self):
        sql = su.compose_abbr_query_statement()
        abbr_df = mc.mysql_export_data_to_df(sql, self.section)
        return abbr_df

    def find_var_type_by_transducer(self, var, df):
        bool_value = df['VAR_TYPE_TRANSDUCER_LIST'].str.contains(var)
        abbr_code_list = df[bool_value].ABBR_CODE.values
        return abbr_code_list

    def init_workable_vars(self, abbr_df):
        # 初始化所有的设备
        self.dic = {}
        city_id_list = pd.unique(self.active_devices_info.CITYID.values)
        for city_id in city_id_list:
            # print(city_id)
            self.dic[city_id] = {}
            city_active_devices = self.active_devices_info[
                self.active_devices_info.CITYID == city_id]
            for i in ['PM25', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'TVOC', 'TSP']:
                # print(i)
                transducer = self.find_var_type_by_transducer(
                    i, abbr_df)
                # print(transducer)
                var_device_list = city_active_devices[
                    city_active_devices.ABBR_CODE.isin(transducer)].DEV_ID.values
                if len(var_device_list) > 0:
                    self.dic[city_id][i] = list(var_device_list)
                else:
                    self.dic[city_id][i] = []

        # print(self.dic)
    
    def query_workable_vars_by_city_list(self, city_list):

        dic_city = {'PM25': [], 'PM10': [], 'SO2': [], 'NO2': [], 'CO': [],
                        'O3': [], 'TVOC': [], 'TSP': []}

        for x in city_list:
            city_var_list = self.dic.get(x)
            for i in ['PM25', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'TVOC', 'TSP']:
                dic_city[i] = dic_city[i]+city_var_list[i]
        return dic_city

    def query_adj_data_within_one_month(self, device_list, start_time, end_time):
        """
        获取当月的ADJ数据
        """
        sql_statement = su.compose_adj_data_query_statement(
            start_time, end_time, device_list)
        adj_data = mc.mysql_export_data_to_df(sql_statement, self.section)
        return adj_data

    def merge_adj_data_cross_month(self, device_list, start_time, end_time):
        # 跨月取数时，对不同月份的数据进行合并
        # 判断是否时同一个月的数据
        if start_time[5:7] == end_time[5:7]:
            adj_data = self.query_adj_data_within_one_month(
                device_list, start_time, end_time)
            return adj_data
        else:
            # 获取上月的月末时间
            month_endtime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.last_day_of_a_month(
                        tu.time_str_to_datetime(start_time))[0]))

            month_starttime = tu.datetime_to_string(
                tu.time_str_to_datetime(
                    tu.first_day_of_a_month(
                        tu.time_str_to_datetime(end_time))[0]))
            # 获取上个月的adj数据
            adj_data_last_month = self.query_adj_data_within_one_month(
                device_list, start_time, month_endtime)
            # 获取当月的adj数据
            adj_data_this_month = self.query_adj_data_within_one_month(
                device_list, month_starttime, end_time)
            # 合并org数据
            if adj_data_last_month is not None:
                adj_data = adj_data_last_month.append(adj_data_this_month)
                return adj_data
            else:
                adj_data_this_month

    def query_adj_data_by_device_list(self, device_list, start_time, end_time):
        """
        获取质控数据
        device_list：设备列表
        start_time：开始时间
        end_time：结束时间
        """
        adj_data = self.merge_adj_data_cross_month(device_list, start_time, end_time)
        return adj_data


class MyManager(BaseManager):
    pass


def manager_dao():
    m = MyManager()
    m.start()
    return m


MyManager.register('DataOperationsByMysql', DataOperationsByMysql)
MyManager.register('QualityControlConfig', qc.QualityControlConfig)

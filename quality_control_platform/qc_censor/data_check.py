import numpy as np
from error_demo.error_code import *
import datetime
from log import log
logger = log.log_demo()


class DataCheck:
    def __init__(self, dao, config, spatial_indexer, device_list_info, city_id, hour, variables):
        self.city = city_id
        self.hour = hour
        self.dao = dao
        self.config = config
        self.spatial_indexer = spatial_indexer
        self.device_list_info = device_list_info
        self.variables = variables
        self.site_distance = self.config.get_config_global_data('max_site_data_check_distance')
        self.low_concentration_standard_dict = self.config_var_dict(self.variables,'low_concentration_standard')
        self.medium_concentration_standard_dict = self.config_var_dict(self.variables,'medium_concentration_standard')
        self.high_concentration_standard_dict = self.config_var_dict(self.variables,'high_concentration_standard')
        self.low_alert_threshold_dict = self.config_var_dict(self.variables,'low_alert_threshold')
        self.medium_alert_threshold_dict = self.config_var_dict(self.variables,'medium_alert_threshold')
        self.high_alert_threshold_dict = self.config_var_dict(self.variables,'high_alert_threshold')
        self.summit_alert_threshold_dict = self.config_var_dict(self.variables,'summit_alert_threshold')
        self.cur_datehour = datetime.datetime.strptime(self.hour,'%Y-%m-%d %H:00:00')
        self.last_hour = (self.cur_datehour-datetime.timedelta(hours=1))
        self.max_dev_data_check_distance = self.config.get_config_global_data('max_dev_data_check_distance')

    def config_var_dict(self, variables, str):
        var_dict = {}
        for var in variables:
            res = self.config.get_config_var_data(str, var)
            try:
                var_dict[var] = res
            except BaseError as e:
                e.setdata({'key': str,'var':var})
                logger.error('code:%s,name:%s,message:%s,data:%s',e.code, e.name,e.message, e.getdata(), exc_info=True)
        return var_dict

    def site_data_check(self, site_data, var):
        """
        子站设备审核，通过查找一定距离范围内附近子站对比判定子站信息是否有误
        :param site_data:df格式的子站数据
        :param var:需要判定的污染物
        :param distance:作为判定的距离
        :return:有异常的子站列表
        """
        low_concentration_standard = self.low_concentration_standard_dict[var]
        medium_concentration_standard = self.medium_concentration_standard_dict[var]
        high_concentration_standard = self.high_concentration_standard_dict[var]
        low_alert_threshold = self.low_alert_threshold_dict[var]
        medium_alert_threshold = self.medium_alert_threshold_dict[var]
        high_alert_threshold = self.high_alert_threshold_dict[var]
        summit_alert_threshold = self.summit_alert_threshold_dict[var]
        logger.info('low_concentration_standard:%s,medium_concentration_standard:%s,''high_concentration_standard:%s,'
                    'low_alert_threshold:%s,medium_alert_threshold:%s,high_alert_threshold:%s,summit_alert_threshold:%s'
                    %(low_concentration_standard,medium_concentration_standard,high_concentration_standard,low_alert_threshold,
                      medium_alert_threshold,high_alert_threshold,summit_alert_threshold))
        site_list = site_data['SITE_ID'].unique().tolist()
        site_error_lyst = []
        for site_id in site_list:
            site_dev_list = self.spatial_indexer.find_nearest_site_by_site(site_id,self.site_distance,True)
            site_id_value, site_var_list = self.site_data_prepare(site_dev_list, site_data, var, self.cur_datehour,self.last_hour,site_id)
            if not np.isnan(site_id_value):
                logger.debug('%s周围子站数值%s，子站本身数值%s' % (site_id, site_var_list, site_id_value))
                if var == 'TSP':
                    res = self.tsp_handle(site_var_list,site_id,site_id_value,low_concentration_standard,medium_concentration_standard,
                                          high_concentration_standard,low_alert_threshold,medium_alert_threshold,high_alert_threshold,
                                          summit_alert_threshold,var,site_dev_list,site_data,self.cur_datehour,self.last_hour)
                    if res:
                        site_error_lyst.append(res)
                else:
                    if len(site_var_list) > 2:
                        res = self.site_data_handle(site_var_list, site_id, site_id_value, low_concentration_standard,
                                                    medium_concentration_standard, high_concentration_standard, low_alert_threshold,
                                                    medium_alert_threshold, high_alert_threshold, summit_alert_threshold,var)
                        if res:
                            site_error_lyst.append(res)
                    else:
                        logger.warning('城市:%s 时间:%s var:%s 子站:%s范围内周围子站少于三个，周围子站为:%s'%(self.city, self.hour, var,site_id,site_var_list))
        logger.debug('城市：%s 时间：%s var:%s 子站设备比周围子站测量值异常设备列表:%s' % (self.city, self.hour, var, site_error_lyst))
        return site_error_lyst

    def tsp_handle(self,site_var_list,site_id,site_id_value,low_concentration_standard,medium_concentration_standard,
                   high_concentration_standard,low_alert_threshold,medium_alert_threshold,high_alert_threshold,
                   summit_alert_threshold,var,site_dev_list,site_data,cur_datehour,last_hour):
        if len(site_var_list) > 2:
            res = self.site_data_handle(site_var_list, site_id, site_id_value, low_concentration_standard, medium_concentration_standard,
                                        high_concentration_standard, low_alert_threshold, medium_alert_threshold, high_alert_threshold,
                                        summit_alert_threshold, var)
            if res:
                if site_id_value>low_concentration_standard:
                    site_PM10_value, site_PM10_list = self.site_data_prepare(site_dev_list, site_data, 'PM10', cur_datehour, last_hour,site_id)
                    logger.info('%sTSP用PM10标准周围子站数值%s，子站本身数值%s' % (site_id, site_PM10_list, site_id_value))
                    if len(site_PM10_list) > 2:
                        site_res = self.tsp_use_pm10_handle(site_PM10_list, site_id, site_id_value)
                        return site_res
                else:
                    return site_id
        else:
            logger.debug('城市:%s 时间:%s var:%s 子站:%s范围内周围子站少于三个，周围子站为:%s' % (self.city, self.hour, var, site_id, site_var_list))
            if site_id_value>low_concentration_standard:
                site_PM10_value, site_PM10_list = self.site_data_prepare(site_dev_list, site_data, 'PM10', cur_datehour,last_hour, site_id)
                logger.debug('%sTSP用PM10标准周围子站数值%s，子站本身数值%s' % (site_id, site_PM10_list, site_id_value))
                if len(site_PM10_list) > 2:
                    site_res = self.tsp_use_pm10_handle(site_PM10_list, site_id, site_id_value)
                    return site_res

    def tsp_use_pm10_handle(self,site_PM10_list,site_id,site_id_value):
        for i in range(len(site_PM10_list)):
            site_PM10_list[i] = site_PM10_list[i] * 2
        PM10_mean = np.mean(site_PM10_list)
        if site_id_value > PM10_mean:
            logger.debug('城市：%s 时间：%s %s子站数据可能有异常，原因：该设备测量值%s大于附近子站PM10平均值%s乘以2，'
                        '周围子站的测量值为%s' % (self.city, self.hour, site_id, site_id_value, PM10_mean, site_PM10_list))
            return site_id

    def site_data_prepare(self,site_dev_list,site_data,var,cur_hour,last_hour,site_id):
        """
        子站本身数据和周围子站数据函数，输入周围子站列表，子站df，污染物类型，当前时间，上小时时间，目标子站编号生成子站本身数值
        和周围子站数值列表
        :param site_dev_list: 周围子站列表
        :param site_data: 子站df
        :param var: 污染物类型
        :param cur_hour: 当前时间
        :param last_hour: 上小时时间
        :param site_id: 目标子站编号
        :return: 子站本身数值和周围子站数值列表
        """
        site_var_list = []
        df = site_data.copy()
        df.set_index(['SITE_ID', 'TIMESTAMP'], inplace=True)
        site_id_value = self.site_dev_data(df,site_id,cur_hour,var)
        if not np.isnan(site_id_value):
            for site in site_dev_list:
                site_value = self.site_dev_data(df,site,cur_hour,var)
                if not np.isnan(site_value):
                    site_var_list.append(site_value)
        else:
            site_id_value = self.site_dev_data(df, site_id, last_hour, var)
            for site in site_dev_list:
                site_value = self.site_dev_data(df,site,last_hour,var)
                if not np.isnan(site_value):
                    site_var_list.append(site_value)
        return site_id_value,site_var_list

    def site_dev_data(self,df,site,time,var):
        """
        根据子站df，子站编号，时间字段，污染物类型获取df中对应污染物数据
        :param df: 子站df
        :param site: 子站编号
        :param time: 时间字段
        :param var: 污染物类型
        :return: df中对应污染物数据
        """
        try:
            df.loc[(site, time), 'SITE_' + var]
        except Exception:
            site_value = np.nan
        else:
            site_value = df.loc[(site, time), 'SITE_' + var]
        return site_value

    def site_data_handle(self, lyst,dev_id,dev_value, low_concentration_standard, medium_concentration_standard,
                           high_concentration_standard, low_alert_threshold, medium_alert_threshold,
                           high_alert_threshold, summit_alert_threshold,var):
        """
        子站审核数据处理函数，通过周围设备浓度获取对应浓度范围的警戒阈值，通过判断设备数据是否超过警戒阈值以及该设备与周围浓度
        的关系判断该设备是否为异常。
        :param lyst: 周围设备的数据列表
        :param dev_id: 待审核的设备编号
        :param dev_value: 待审核的设备数据
        :param low_concentration_standard: 低浓度范围标准值
        :param medium_concentration_standard: 中浓度范围标准值
        :param high_concentration_standard: 高浓度范围标准值
        :param low_alert_threshold: 低浓度警戒阈值
        :param medium_alert_threshold: 中浓度警戒阈值
        :param high_alert_threshold: 高浓度警戒阈值
        :param summit_alert_threshold: 最高警戒阈值
        :param var: 污染物类型
        :return: 异常子站编号
        """
        dev_max = np.max(lyst)
        dev_min = np.min(lyst)
        lyst.remove(dev_max)
        lyst.remove(dev_min)
        dev_mean = np.mean(lyst)
        if dev_mean < low_concentration_standard:
            if dev_value > (dev_mean + low_alert_threshold):
                logger.debug('城市：%s 时间：%s var:%s %s子站数据可能有异常，原因：该设备测量值%s大于附近子站平均值%s加上系数%s，'
                        '周围子站的测量值为%s' % (self.city, self.hour, var, dev_id, dev_value, dev_mean, low_alert_threshold, lyst))
                return dev_id
            elif dev_value < (dev_mean - low_alert_threshold):
                logger.debug('城市：%s 时间：%s var:%s %s子站数据有异常，原因：该子站测量值%s小于附近子站平均值%s减去系数%s，'
                            '周围子站的测量值为%s'% (self.city, self.hour, var, dev_id, dev_value, dev_mean, low_alert_threshold, lyst))
                return dev_id
        else:
            if dev_mean < medium_concentration_standard:
                concentration_standard = medium_alert_threshold
            elif dev_mean < high_concentration_standard:
                concentration_standard = high_alert_threshold
            else:
                concentration_standard = summit_alert_threshold
            if dev_value > dev_mean * (1 + concentration_standard):
                logger.debug('城市：%s 时间：%s var:%s %s子站数据可能有异常，原因：该子站测量值%s大于附近子站平均值%s乘以系数%s，'
                        '周围子站的测量值为%s'% (self.city, self.hour, var, dev_id, dev_value, dev_mean, concentration_standard, lyst))
                return dev_id
            elif dev_value < dev_mean * (1 - concentration_standard):
                logger.debug('城市：%s 时间：%s var:%s %s子站数据有异常，原因：该子站测量值%s小于附近子站平均值%s乘以系数%s，'
                        '周围子站的测量值为%s'% (self.city, self.hour, var, dev_id, dev_value, dev_mean, concentration_standard, lyst))
                return dev_id

    def qc_data_check(self, qc_df, var):
        """
        质控设备审核，通过查找同一点位下的质控设备对比判断该质控设备是否有异常
        :param qc_df: df格式的质控设备数据
        :param var: 作为判断的参数
        :return: 有异常的质控设备列表
        """
        dev_list = qc_df['DEV_ID'].unique().tolist()
        qc_df.set_index(['DEV_ID', 'VAR'], inplace=True)
        qc_error_lyst = []
        for dev_id in dev_list:
            dev_var_list = []
            try:
                qc_df.loc[(dev_id, var), 'ADJ_VALUE']
            except Exception:
                continue
            else:
                dev_value = qc_df.loc[(dev_id, var), 'ADJ_VALUE']
            if not self.device_list_info[self.device_list_info['DEV_ID'] ==dev_id].empty:
                dev_measure_point_id = self.device_list_info[self.device_list_info['DEV_ID'] ==dev_id]['MEASURE_POINT_ID'].values[0]
                dev_point_list = self.device_list_info[self.device_list_info['MEASURE_POINT_ID'] == dev_measure_point_id]['DEV_ID'].unique().tolist()
                for dev in dev_point_list:
                    try:
                        qc_df.loc[(dev, var), 'ADJ_VALUE']
                    except Exception:
                        continue
                    else:
                        dev_values = qc_df.loc[(dev, var), 'ADJ_VALUE']
                        dev_var_list.append(dev_values)
            logger.debug('城市:%s 时间:%s 设备~~~%s污染物%s数值为%s,周围设备列表为%s,数值为%s' % (self.city, self.hour, dev_id, var, dev_value, dev_point_list, dev_var_list))
            if len(dev_var_list)>2:
                res = self.qc_data_handle(dev_var_list,dev_value,dev_id,var)
                if res:
                    qc_error_lyst.append(res)
        logger.debug('城市：%s 时间：%s var：%s 质控设备比周围设备测量值低的异常设备列表:%s' %(self.city, self.hour, var, qc_error_lyst))
        return qc_error_lyst

    def qc_data_handle(self,lyst,dev_value,dev_id,var):
        """
        质控设备数据处理，判断当前质控设备与同点位的数据差值是否超过标准差。如果超过认定该质控设备为异常。
        :param lyst: 同点位下质控设备数值列表
        :param dev_value: 当前质控设备的数据
        :param dev_id: 当前质控设备编号
        :return: 有异常的质控设备编号
        """
        dev_sd = np.std(lyst, ddof=1)
        dev_mean = np.mean(lyst)
        if dev_value < dev_mean - abs(dev_sd):
            logger.debug('城市：%s 时间：%s var：%s ~~~%s质控设备数据有异常，原因：该设备测量值%s小于附近设备平均值%s减上标准差%s'
                           '，周围设备的测量值为%s' % (self.city, self.hour, var, dev_id, dev_value, dev_mean, dev_sd, lyst))
            return dev_id
        elif dev_value > dev_mean + abs(dev_sd):
            logger.debug('城市：%s 时间：%s var：%s ~~~%s质控设备数据可能有异常，原因：该设备测量值%s大于附近设备平均值%s加上标准差%s'
                           '，周围设备的测量值为%s' % (self.city, self.hour, var, dev_id, dev_value, dev_mean, dev_sd, lyst))
            return dev_id

    def non_qc_data_check(self, non_qc_data, var):
        """
        非质控设备的审核，通过判断与周围附近5台设备比较判断该非质控设备是否异常。
        :param non_qc_data: df格式的非质控设备数据
        :param var: 作为判断的参数
        :return: 有异常的非质控设备列表
        """
        low_concentration_standard = self.low_concentration_standard_dict[var]
        medium_concentration_standard = self.medium_concentration_standard_dict[var]
        high_concentration_standard = self.high_concentration_standard_dict[var]
        low_alert_threshold = self.low_alert_threshold_dict[var]
        medium_alert_threshold = self.medium_alert_threshold_dict[var]
        high_alert_threshold = self.high_alert_threshold_dict[var]
        summit_alert_threshold = self.summit_alert_threshold_dict[var]
        logger.debug('low_concentration_standard:%s,medium_concentration_standard:%s,high_concentration_standard:%s,'
                    'low_alert_threshold:%s,medium_alert_threshold:%s,high_alert_threshold:%s,summit_alert_threshold:%s'
                    %(low_concentration_standard,medium_concentration_standard,high_concentration_standard,low_alert_threshold,
                      medium_alert_threshold,high_alert_threshold,summit_alert_threshold))
        non_dev_lyst = non_qc_data['DEV_ID'].unique()
        non_qc_data.set_index(['DEV_ID', 'VAR'], inplace=True)
        non_error_lyst = []
        for dev_id in non_dev_lyst:
            non_list = []
            try:
                non_qc_data.loc[(dev_id, var), 'ADJ_VALUE']
            except Exception:
                continue
            else:
                dev_value = non_qc_data.loc[(dev_id, var), 'ADJ_VALUE']
            # non_var_list = self.spatial_indexer.find_nearest_devices(dev_id,var,5)
            non_var_list = self.spatial_indexer.find_dev_by_distance(var, dev_id, self.max_dev_data_check_distance)
            if non_var_list is not None:
                for dev in non_var_list:
                    try:
                        non_qc_data.loc[(dev, var), 'ADJ_VALUE']
                    except Exception:
                        continue
                    else:
                        dev_values = non_qc_data.loc[(dev, var), 'ADJ_VALUE']
                        non_list.append(dev_values)
            logger.debug('城市：%s 时间：%s %s设备%s污染物数值为%s,周围设备列表为%s,数值为%s'%(self.city,self.hour,dev_id,var,dev_value,non_var_list,non_list))
            if len(non_list) >= 2:
                res = self.non_qc_data_handle(non_list,dev_id, dev_value, low_concentration_standard,
                                              medium_concentration_standard, high_concentration_standard,
                                              low_alert_threshold, medium_alert_threshold,
                                              high_alert_threshold, summit_alert_threshold,var)
                if res:
                    non_error_lyst.append(res)
        logger.debug('城市：%s 时间：%s var:%s 非质控设备比周围设备测量值低的异常设备列表:%s' % (self.city, self.hour, var,non_error_lyst))
        return non_error_lyst

    def non_qc_data_handle(self, lyst,dev_id,dev_value, low_concentration_standard, medium_concentration_standard,
                           high_concentration_standard, low_alert_threshold, medium_alert_threshold,high_alert_threshold,
                           summit_alert_threshold,var):
        """
        非质控设备数据处理，通过周围设备浓度获取对应浓度范围的警戒阈值，通过判断设备数据是否超过警戒阈值以及该设备与周围浓度
        的关系判断该设备是否为异常。
        :param lyst:周围设备的数据列表
        :param dev_id:待审核的设备编号
        :param dev_value:待审核的设备数据
        :param low_concentration_standard:低浓度范围标准值
        :param medium_concentration_standard:中浓度范围标准值
        :param high_concentration_standard:高浓度范围标准值
        :param low_alert_threshold:低浓度警戒阈值
        :param medium_alert_threshold:中浓度警戒阈值
        :param high_alert_threshold:高浓度警戒阈值
        :param summit_alert_threshold:最高警戒阈值
        :return:异常设备编号
        """
        dev_mean = np.mean(lyst)
        if dev_mean < low_concentration_standard:
            if dev_value > (dev_mean + low_alert_threshold):
                logger.debug('城市：%s 时间：%s var:%s %s非质控设备数据可能有异常，原因：该设备测量值%s大于附近设备平均值%s加上系数%s或者大于'
                            '周围设备，周围设备的测量值为%s'%(self.city,self.hour,var,dev_id, dev_value,dev_mean,low_alert_threshold,lyst))
            elif dev_value < (dev_mean - low_alert_threshold):
                logger.debug('城市：%s 时间：%s var:%s %s非质控设备数据有异常，原因：该设备测量值%s小于附近设备平均值%s减去系数%s或者小于周围'
                            '设备，周围设备的测量值为%s'% (self.city,self.hour,var,dev_id, dev_value, dev_mean, low_alert_threshold, lyst))
                return dev_id
        else:
            if dev_mean < medium_concentration_standard:
                concentration_standard = medium_alert_threshold
            elif dev_mean < high_concentration_standard:
                concentration_standard = high_alert_threshold
            else:
                concentration_standard = summit_alert_threshold
            if dev_value > dev_mean * (1 + concentration_standard):
                logger.debug('城市：%s 时间：%s var:%s %s非质控设备数据可能有异常，原因：该设备测量值%s大于附近设备平均值%s乘以系数%s或者大于'
                        '周围设备，周围设备的测量值为%s'%(self.city,self.hour,var,dev_id,dev_value,dev_mean,concentration_standard,lyst))
            elif dev_value < dev_mean * (1 - concentration_standard):
                logger.debug('城市：%s 时间：%s var:%s %s非质控设备数据有异常，原因：该设备测量值%s小于附近设备平均值%s乘以系数%s或者小于周围'
                            '设备，周围设备的测量值为%s'% (self.city,self.hour,var,dev_id,dev_value,dev_mean,concentration_standard, lyst))
                return dev_id


if __name__ == '__main__':
    from dao.mysql_impl import DataOperationsByMysql
    from config.qc_config import QualityControlConfig
    from utility.neighbor_devices import NeighborDevices
    city_id = [1]
    hour = '2018-11-01 00:00:00'
    config = QualityControlConfig()
    dao = DataOperationsByMysql(config,hour)
    device_list_info = dao.query_active_devices_by_city(city_id)
    device_list = device_list_info['DEV_ID'].unique().tolist()
    spatial_indexer = NeighborDevices(device_list)
    datacheck = DataCheck(dao,config,spatial_indexer, city_id, hour)
    qc_data = dao.query_qc_dev_org_data_by_city(city_id, hour)
    non_qc_data = dao.query_non_qc_dev_org_data_by_city(city_id, hour)
    print(len(qc_data))
    print(datacheck.qc_data_check(qc_data, 'PM25'))
    print(datacheck.non_qc_data_check(non_qc_data, 'PM25'))


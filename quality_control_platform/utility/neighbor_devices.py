# encoding = utf-8
import common
from rtree import index
from math import radians, cos, sin, asin, sqrt
from aux_entities.vargroup_qc_versions import VargroupQCVersions
from log import log
from error_demo.error_code import *
logger = log.log_demo()


class NeighborDevices(object):
    """
    利用rtree包管理距离及寻找最近的设备
    http://toblerity.org/rtree/tutorial.html#using-rtree-as-a-cheapo-spatial-database
    """

    def __init__(self,dao, device_list=None, city_id=None):

        vg_versions = VargroupQCVersions(dao)
        version_dict = vg_versions.qc_vargroup_by_versions_and_var

        # 质控设备对应的df数据
        self.qc_device_info = self.sql_data_devices(dao, True, device_list, city_id)

        # 非质控设备对应的df数据
        self.all_qc_device_info = self.sql_data_devices(dao, False, device_list, city_id)

        # 参数字典
        self.var_dict = self.get_data(version_dict)

        # 全局参数字典
        self.all_var_dict = self.get_all_data(version_dict)

        # 计算每个site所挂的设备数
        self.num_device_per_site_dict = self.gen_num_device_per_site()

        # 子站对应的rtree数据
        self.idx_site_device = self.site_sql_df(dao)

    def gen_num_device_per_site(self):
        """
        计算每个site所挂的设备数
        :return: 每个site所挂的设备数字典
        """
        num_device_per_site = self.qc_device_info.groupby(self.qc_device_info['MEASURE_POINT_ID']).count()
        num_device_per_site.reset_index(inplace=True)
        measure_point_ids = num_device_per_site['MEASURE_POINT_ID'].unique()
        num_device_per_site_dict = {}
        for measure_point_id in measure_point_ids:
            num_site = num_device_per_site[num_device_per_site['MEASURE_POINT_ID'] == measure_point_id]['DEV_ID'].values[0]
            num_device_per_site_dict[measure_point_id] = num_site
        return num_device_per_site_dict

    def sql_data_devices(self, dao, flag, device_list=None, city_id=None):

        """
        从数据库读取的空间坐标和设备编号
        :param flag: 是否为质控
        :return: 返回相对应的sql数据
        """
        if flag:
            sql_data = dao.query_devices_latitude_longitude(1, device_list=device_list, city_id=city_id)
        else:
            sql_data = dao.query_devices_latitude_longitude(device_list=device_list, city_id=city_id)
        return sql_data

    def idx_devices(self, data):
        """
        将质控设备df数据生成rtree数据
        :return:质控设备 rtree数据
        """
        idx = index.Index()
        for i in range(len(data['DEV_ID'])):
            GOOGLELONGITUDE, GOOGLELATITUDE = (data.loc[i]['GOOGLELONGITUDE'],
                                               data.loc[i]['GOOGLELATITUDE'])
            idx.insert(i, (GOOGLELONGITUDE, GOOGLELATITUDE),obj=data.loc[i]['DEV_ID'])
        return idx

    def devices_handle(self, dev_id, idx, index, site, num_neighbors,needless_num,if_excl_same_site=False):
        """
        通过rtree包的nearest方法获取最近设备的id
        :param dev_id: 目标设备编号
        :param idx: 对用类型的rtree数据
        :param index: 对应质控类型的index的df数据
        :param site: 对应质控类型的site的df数据
        :param num_neighbors: 需要返回的邻居设备数量
        :param needless_num: 需要排除的设备数量
        :return: 返回邻居设备编号的列表
        """
        dev_measure_point_id = site[site['DEV_ID'] == dev_id]['MEASURE_POINT_ID'].values[0]
        dev_longitude = site[site['DEV_ID'] == dev_id]['GOOGLELONGITUDE'].values[0]
        dev_latitude = site[site['DEV_ID'] == dev_id]['GOOGLELATITUDE'].values[0]
        res = list(idx.nearest((dev_longitude, dev_latitude), num_neighbors + needless_num))
        dev_lyst = []
        distance_lyst = []
        for i in res:
            if if_excl_same_site:
                measure_point_id = index.loc[i]['MEASURE_POINT_ID']
                if measure_point_id == dev_measure_point_id:
                    continue
            else:
                dev = index.loc[i]['DEV_ID']
                if dev == dev_id:
                    continue
            dev_lyst.append(index.loc[i]['DEV_ID'])
            lon = index.loc[i]['GOOGLELONGITUDE']
            lat = index.loc[i]['GOOGLELATITUDE']
            distance = self.haversine(dev_longitude, dev_latitude, lon, lat)
            distance_lyst.append(distance)
        res_dict = {}
        for j in range(len(distance_lyst)):
            res_dict[dev_lyst[j]] = distance_lyst[j]
        return res_dict

    def get_data(self, version_dict):
        """
        生成三层字典，第一层key为var，第二层key为version_id，第三层为对应的qc_data，all_qc_data，idx_qc，idx_all_qc
        :param version_dict: 传进来的vergroup_id，var，version构成的 三层字典
        :return: 返回进行数据处理的三层字典
        """
        var_dict = {}
        for var, var_version_dict in version_dict.items():
            version_dict={}
            for version_id, vargroup_list in var_version_dict.items():
                idx_dict = {}
                qc_dev_data = self.qc_device_info[(self.qc_device_info['VARGROUP_ID'].isin(vargroup_list)) & (self.qc_device_info['RELATE_SITE_ID'] > 0)].copy()
                all_qc_dev_data = self.all_qc_device_info[(self.all_qc_device_info['VARGROUP_ID'].isin(vargroup_list)) & (self.all_qc_device_info['RELATE_SITE_ID'] >= -1)].copy()

                qc_dev_data.reset_index(inplace=True)
                all_qc_dev_data.reset_index(inplace=True)

                idx_qc = self.idx_devices(qc_dev_data)
                idx_all_qc = self.idx_devices(all_qc_dev_data)
                idx_dict['qc_data'] = qc_dev_data
                idx_dict['all_qc_data'] = all_qc_dev_data
                idx_dict['idx_qc'] = idx_qc
                idx_dict['idx_all_qc'] = idx_all_qc
                version_dict[version_id] = idx_dict

            var_dict[var] = version_dict
        return var_dict

    def get_all_data(self, version_dict):
        """
        生成二层字典，第一层key为var，第二层为对应的all_qc_data，idx_all_qc
        :param version_dict: 传进来的vergroup_id，var，version构成的 三层字典
        :return: 返回进行数据处理的二层字典
        """
        var_dict = {}
        for var, var_version_dict in version_dict.items():
            vargroup_lyst = []
            for version_id, vargroup_list in var_version_dict.items():
                vargroup_lyst += vargroup_list
            idx_dict = {}
            all_qc_dev_data = self.all_qc_device_info[(self.all_qc_device_info['VARGROUP_ID'].isin(vargroup_lyst)) & (self.all_qc_device_info['RELATE_SITE_ID'] >= -1)].copy()
            all_qc_dev_data.reset_index(inplace=True)
            idx_all_qc = self.idx_devices(all_qc_dev_data)
            idx_dict['all_qc_data'] = all_qc_dev_data
            idx_dict['idx_all_qc'] = idx_all_qc
            var_dict[var] = idx_dict
        return var_dict

    def find_nearest_qc_devices(self, dev_id, version, var_name, num_neighbors, if_excl_same_site):
        """
        给定设备编号,版本，参数，数量，是否排除同点位，寻找最近的质控设备
        :param dev_id: 设备编号
        :param version: 版本
        :param var_name: 参数
        :param num_neighbors: 需要返回的邻居设备数量
        :param if_excl_same_site: 如果输入的dev_id是质控设备
        :return: 最近的设备与距离构成的字典
        """
        idx_dict = self.var_dict[var_name][version]
        qc_data = idx_dict['qc_data']
        idx_qc = idx_dict['idx_qc']
        all_qc_data = idx_dict['all_qc_data']

        if if_excl_same_site and dev_id in qc_data['DEV_ID'].unique():
            cur_measure_point_id = qc_data[qc_data['DEV_ID'] == dev_id]['MEASURE_POINT_ID'].values[0]
            needless_num = self.num_device_per_site_dict[cur_measure_point_id]
            res = self.devices_handle(dev_id, idx_qc, qc_data, qc_data, num_neighbors, needless_num,if_excl_same_site=True)
            return res

        elif dev_id in all_qc_data['DEV_ID'].unique():
            needless_num = 1
            res = self.devices_handle(dev_id, idx_qc, qc_data, all_qc_data, num_neighbors, needless_num)
            return res
        else:
            logger.info('寻找最近质控设备，输入设备编号nnn{}不在这个version{}下'.format(dev_id, var_name))

    def find_nearest_devices(self, dev_id, var_name, num_neighbors):
        """
        给定设备编号,参数，数量，寻找最近的设备
        :param dev_id: 设备编号
        :param var_name: 参数
        :param num_neighbors: 需要返回的邻居设备数量
        :return: 最近的设备与距离构成的字典
        """
        idx_dict_versions = self.all_var_dict[var_name]
        idx_all_qc = idx_dict_versions['idx_all_qc']
        all_qc_data = idx_dict_versions['all_qc_data']
        if dev_id in list(all_qc_data['DEV_ID']):
            needless_num = 1
            res = self.devices_handle(dev_id, idx_all_qc, all_qc_data, all_qc_data, num_neighbors, needless_num)
            return res
        else:
            logger.info('寻找最近设备，输入设备编号nnn{}不在该version{}中'.format(dev_id, var_name))

    def haversine(self, lon1, lat1, lon2, lat2):
        # 将十进制度数转化为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371
        return c * r * 1000

    def site_sql_df(self,dao):
        """
        读取数据库获取子站设备号，经度，纬度并转换为rtree类型
        :param dao: 读取数据库接口需要引用的包
        :return: 所有子站的rtree数据
        """
        self.site_data = dao.query_site_latitude_longitude()
        idx_site_device = self.idx_site(self.site_data)
        return idx_site_device

    def idx_site(self, data):
        """
        将子站设备df数据生成rtree数据
        :return:质控设备 rtree数据
        """
        idx = index.Index()
        for i in range(len(data['SITE_ID'])):
            GOOGLELONGITUDE, GOOGLELATITUDE = (data.loc[i]['LONGITUDE'],
                                               data.loc[i]['LATITUDE'])
            idx.insert(i, (GOOGLELONGITUDE, GOOGLELATITUDE),obj=data.loc[i]['SITE_ID'])
        return idx

    def find_nearest_site_by_distance(self,dev_id,distance):
        """
        给定查找距离寻找范围内所有子站
        :param dev_id: 目标设备编号
        :param distance: 查找距离  单位：公里
        :return: 一个字典，key：设备编号  value：与目标设备的距离 单位：米
        """
        site_longitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELONGITUDE'].values[0]
        site_latitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELATITUDE'].values[0]
        # 同经度下，1纬度相差111公里。利用距离/111计算纬度的上下限
        min_latitude = site_latitude-distance/111
        max_latitude = site_latitude+distance/111
        # 同纬度下，由于不同纬度是不同大小的同心圆，半径为地球半径*cos纬度。故经度差为距离/(111*cos纬度)
        margin_longitude = distance/(111*cos(site_latitude))
        min_longitude = site_longitude-abs(margin_longitude)
        max_longitude = site_longitude+abs(margin_longitude)
        hits = list(self.idx_site_device.intersection((min_longitude,min_latitude,max_longitude,max_latitude),objects=True))
        dev_lyst = []
        distance_lyst = []
        for i in hits:
            dev = int(i.object)
            dev_lyst.append(dev)
            lon = i.bounds[1]
            lat = i.bounds[2]
            site_distance = self.haversine(site_longitude, site_latitude, lon, lat)
            distance_lyst.append(site_distance)
        res_dict = {}
        for j in range(len(distance_lyst)):
            res_dict[dev_lyst[j]] = distance_lyst[j]
        return res_dict

    def find_nearest_site_by_num(self,dev_id,num_neighbors):
        """
        给定查找距离寻找范围内所有子站
        :param dev_id: 目标设备编号
        :param distance: 查找距离  单位：公里
        :return: 一个字典，key：设备编号  value：与目标设备的距离 单位：米
        """
        if dev_id in list(self.all_qc_device_info['DEV_ID']):
            dev_longitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELONGITUDE'].values[0]
            dev_latitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELATITUDE'].values[0]
            hits = list(self.idx_site_device.nearest((dev_longitude, dev_latitude), num_neighbors,objects=True))
            dev_lyst = []
            distance_lyst = []
            for i in hits:
                dev = i.object
                dev_lyst.append(int(dev))
                lon = i.bounds[1]
                lat = i.bounds[2]
                site_distance = self.haversine(dev_longitude, dev_latitude, lon, lat)
                distance_lyst.append(site_distance)
            res_dict = {}
            for j in range(len(distance_lyst)):
                res_dict[dev_lyst[j]] = distance_lyst[j]
            return res_dict
        else:
            logger.info('寻找最近设备，输入设备编号nnn{}不存在'.format(dev_id))

    def find_nearest_site_by_site(self,site_id,distance,flag):
        """
        给定查找距离寻找范围内所有子站
        :param dev_id: 目标设备编号
        :param distance: 查找距离  单位：公里
        :return: 一个字典，key：设备编号  value：与目标设备的距离 单位：米
        """
        site_longitude = self.site_data[self.site_data['SITE_ID'] == site_id]['LONGITUDE'].values[0]
        site_latitude = self.site_data[self.site_data['SITE_ID'] == site_id]['LATITUDE'].values[0]
        # 同经度下，1纬度相差111公里。利用距离/111计算纬度的上下限
        min_latitude = site_latitude-distance/111
        max_latitude = site_latitude+distance/111
        # 同纬度下，由于不同纬度是不同大小的同心圆，半径为地球半径*cos纬度。故经度差为距离/(111*cos纬度)
        margin_longitude = distance/(111*cos(site_latitude))
        min_longitude = site_longitude-abs(margin_longitude)
        max_longitude = site_longitude+abs(margin_longitude)
        hits = list(self.idx_site_device.intersection((min_longitude,min_latitude,max_longitude,max_latitude),objects=True))
        dev_lyst = []
        for i in hits:
            dev = int(i.object)
            if flag:
                if dev != site_id:
                    dev_lyst.append(dev)
            else:
                dev_lyst.append(dev)
        return dev_lyst

    def find_dev_by_distance(self,var_name,dev_id,distance):
        '''
        给定查找距离寻找范围内所有设备
        :param var_name:污染物名称
        :param dev_id:目标设备号
        :param distance:需要查找的距离
        :return:距离范围内所有的设备列表
        '''
        idx_dict_versions = self.all_var_dict[var_name]
        idx_all_qc = idx_dict_versions['idx_all_qc']
        site_longitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELONGITUDE'].values[
            0]
        site_latitude = self.all_qc_device_info[self.all_qc_device_info['DEV_ID'] == dev_id]['GOOGLELATITUDE'].values[0]
        # 同经度下，1纬度相差111公里。利用距离/111计算纬度的上下限
        min_latitude = site_latitude - distance / 111
        max_latitude = site_latitude + distance / 111
        # 同纬度下，由于不同纬度是不同大小的同心圆，半径为地球半径*cos纬度。故经度差为距离/(111*cos纬度)
        margin_longitude = distance / (111 * cos(site_latitude))
        min_longitude = site_longitude - abs(margin_longitude)
        max_longitude = site_longitude + abs(margin_longitude)
        hits = list(idx_all_qc.intersection((min_longitude, min_latitude, max_longitude, max_latitude),objects=True))
        dev_lyst = []
        distance_lyst = []
        for i in hits:
            dev = i.object
            if dev != dev_id:
                dev_lyst.append(dev)
                lon = i.bounds[1]
                lat = i.bounds[2]
                site_distance = self.haversine(site_longitude, site_latitude, lon, lat)
                distance_lyst.append(site_distance)
        res_dict = {}
        for j in range(len(distance_lyst)):
            res_dict[dev_lyst[j]] = distance_lyst[j]
        return res_dict


def main():
    import common
    from dao.mysql_impl import DataOperationsByMysql
    from config.qc_config import QualityControlConfig
    hour = '2018-11-14 01:00:00'
    config = QualityControlConfig()
    dao = DataOperationsByMysql(config,hour)
    nei_dev = NeighborDevices(dao,city_id=[1])
    # print(nei_dev.find_nearest_site('YSRDPM250000004796',4))
    # print(nei_dev.find_dev_by_distance('PM25','YSRDPM10P500000050',1))
    non_var_list = nei_dev.find_nearest_site_by_num('YSRDPM10P500000050',5)
    print(non_var_list)
    # print(nei_dev.find_dev_by_distance('PM25','YSRDPM10P500000050',1.5))
    # print(nei_dev.site_data)
    # print(nei_dev.find_nearest_site_by_site(10,5))


if __name__ == '__main__':
    main()

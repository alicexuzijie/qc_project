# encoding = utf-8
import common
from dao.mysql_impl import DataOperationsByMysql
from rtree import index


class NeighborDevices():
    """
    利用rtree包管理距离及寻找最近的设备
    http://toblerity.org/rtree/tutorial.html#using-rtree-as-a-cheapo-spatial-database
    """

    def __init__(self):
        self.index_to_device = self.sql_data_devices(True)[0]
        self.device_to_index = self.sql_data_devices(False)[0]
        self.device_to_site = self.sql_data_devices(False)[1]
        self.site_to_devices = self.sql_data_devices(True)[1]

        self.p = index.Property()
        self.p.dimension = 3
        self.idx = index.Index(properties=self.p)
        self.load_data()

    def find_nearest_qc_devices(self, dev_id, num_neighbors, if_excl_same_site):
        """
        给定设备编号，寻找最近的质控设备
        Args:
            dev_id: 设备编号
            num_neighbors: 需要返回的邻居设备数量
            if_excl_same_site: 如果输入的dev_id是质控设备，是否排除同点位设备
        """
        if if_excl_same_site and dev_id in self.site_to_devices:
            for key, value in self.index_to_device.items():
                if self.site_to_devices[key]['MEASURE_POINT_ID'] != self.site_to_devices[dev_id]['MEASURE_POINT_ID']:
                    GOOGLELONGITUDE,GOOGLELATITUDE,ALTITUDE = (self.site_to_devices[key]['GOOGLELONGITUDE'],
                                                      self.site_to_devices[key]['GOOGLELATITUDE'],
                                                      self.site_to_devices[key]['ALTITUDE'])
                    self.idx.insert(value, (GOOGLELONGITUDE, GOOGLELATITUDE,ALTITUDE))
        else:
            for key, value in self.index_to_device.items():
                if key != dev_id:
                    GOOGLELONGITUDE,GOOGLELATITUDE,ALTITUDE = (self.site_to_devices[key]['GOOGLELONGITUDE'],
                                                               self.site_to_devices[key]['GOOGLELATITUDE'],
                                                               self.site_to_devices[key]['ALTITUDE'])
                    self.idx.insert(value, (GOOGLELONGITUDE, GOOGLELATITUDE,ALTITUDE))
        if dev_id in self.index_to_device:
            res = self.devices_handle(dev_id,self.index_to_device,self.site_to_devices,num_neighbors)
            return res
        elif dev_id in self.device_to_index:
            res = self.devices_handle(dev_id,self.index_to_device, self.device_to_site, num_neighbors)
            return res
        else:
            print('输入设备编号不存在')

    def devices_handle(self,dev_id,index,site,num_neighbors):
        """
        通过rtree包的nearest方法获取最近设备的id
        :param dev_id: 目标设备编号
        :param index: 对应质控类型的index字典
        :param site: 对应质控类型的site字典
        :param num_neighbors: 需要返回的邻居设备数量
        :return: 返回邻居设备编号的列表
        """
        dev_longitude = site[dev_id]['GOOGLELONGITUDE']
        dev_latitude = site[dev_id]['GOOGLELATITUDE']
        dev_altitude = site[dev_id]['ALTITUDE']
        res = list(self.idx.nearest((dev_longitude, dev_latitude,dev_altitude), num_neighbors))
        lyst = []
        for i in res:
            lyst.append(self.get_keys(index, i)[0])
        return lyst

    def get_keys(self,d, value):
        """
        通过字典值反向查找字典的键
        :param d: 字典名称
        :param value: 字典的值
        :return: 返回字典的键
        """
        return [k for k, v in d.items() if v == value]

    def find_nearest_non_qc_devices(self, dev_id, num_neighbors):
        """
        给定设备编号，寻找最近的非质控设备

        Args:
            dev_id: 设备编号
            num_neighbors: 需要返回的邻居设备数量
        """
        for key,value in self.device_to_index.items():
            if key != dev_id:
                GOOGLELONGITUDE,GOOGLELATITUDE,ALTITUDE = (self.device_to_site[key]['GOOGLELONGITUDE'],
                                                           self.device_to_site[key]['GOOGLELATITUDE'],
                                                           self.device_to_site[key]['ALTITUDE'])
                self.idx.insert(value, (GOOGLELONGITUDE, GOOGLELATITUDE,ALTITUDE))
        if dev_id in self.index_to_device:
            res = self.devices_handle(dev_id,self.device_to_index,self.site_to_devices,num_neighbors)
            return res
        elif dev_id in self.device_to_index:
            res = self.devices_handle(dev_id,self.device_to_index, self.device_to_site, num_neighbors)
            return res
        else:
            print('输入设备编号不存在')

    def load_data(self):
        pass

    def sql_data_devices(self,flag):
        """
        将数据库读取的空间坐标和设备编号生成字典，第一个字典键为SENSOR_ID，值为df对应的id
        第二个字典键为SENSOR_ID，值为GOOGLELONGITUDE，GOOGLELATITUDE，ALTITUDE，MEASURE_POINT_ID
        组成的二维字典
        :param flag: 是否为质控
        :return: 返回相对应的字典储存在属性中
        """
        if flag:
            db = DataOperationsByMysql()
            sql_data = db.query_devices_latitude_longitude(1)
        else:
            db = DataOperationsByMysql()
            sql_data = db.query_devices_latitude_longitude(-1)
        index_to_device={}
        site_to_device={}
        for i in range(len(sql_data['SENSOR_ID'])):
            key=sql_data['SENSOR_ID'].values[i]
            value_data_index = i
            value_data_site = sql_data[sql_data['SENSOR_ID'] == key]
            value_site = {'GOOGLELONGITUDE':value_data_site['GOOGLELONGITUDE'].values[0],
                          'GOOGLELATITUDE':value_data_site['GOOGLELATITUDE'].values[0],
                          'ALTITUDE':value_data_site['ALTITUDE'].values[0],
                          'MEASURE_POINT_ID':value_data_site['MEASURE_POINT_ID'].values[0]}
            index_to_device[key] = value_data_index
            site_to_device[key] = value_site
        return [index_to_device,site_to_device]


def main():
    ass=NeighborDevices()
    print(ass.find_nearest_qc_devices('YSRDPM250000005342',1,True))
    # print(ass.find_nearest_non_qc_devices('YSRDPM250000002554',1))


if __name__ == '__main__':
    main()

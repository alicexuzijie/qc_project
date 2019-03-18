import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
from utility.neighbor_devices import NeighborDevices
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class Analyze:
    def __init__(self,hour):
        self.config = QualityControlConfig()
        self.dao = DataOperationsByMysql(self.config,hour)
        self.spatial_indexer = NeighborDevices(self.dao,city_id=[1])
        self.root_df = self.get_root_df()
        # self.lon_lat = self.handle()

    def query_dev_id(self,lon,lat):
        res = self.spatial_indexer.find_dev_id_by_distance('PM25',1.5,lon,lat)
        return res

    def make_df(self):
        lon = [116.244088, 116.203945, 116.279166, 116.289122, 116.311811, 116.361036, 116.383171, 116.413232,
               116.420953, 116.221624, 116.200128, 116.157532, 116.354344, 116.414607, 116.401258, 116.372606,
               116.399236, 116.277965, 116.280328, 116.413209, 116.108882, 116.094241, 116.307636, 116.078542,
               116.18933, 116.238472]
        lat = [39.868983, 39.825183, 39.850605, 39.815659, 39.884692, 39.856232, 39.849999, 39.861584, 39.860299,
               39.839904, 39.82954, 39.809722, 39.833401, 39.805558, 39.839109, 39.804785, 39.810009, 39.883958,
               39.812796, 39.832065, 39.861216, 39.787781, 39.785585, 39.859081, 39.802111, 39.8888]
        name = ['小屯公园', '长辛店', '丰台街道', '新村街道', '太平桥街道', '右安门街道', '西罗园街道', '东铁匠营街道', '方庄地区', '宛平地区', '长辛店街道', '云岗街道',
                '马家堡街道', '东高地街道', '大红门街道', '南苑街道', '和义街道', '卢沟桥乡', '花乡', '南苑乡', '长辛店镇', '王佐镇', '羊坊村盛芳花卉', '千灵山', '赵辛店村',
                '小瓦窑村']
        dict = {'name': name, 'lon': lon, 'lat': lat}
        df = pd.DataFrame(dict)
        return df

    def handle(self):
        df = self.make_df()
        dict = {}
        self.city_lyst = list(df['name'].unique())
        global_lyst = []
        global_root_lyst = []
        for city in self.city_lyst:
            lon = df[df['name'] == city]['lon'].values[0]
            lat = df[df['name'] == city]['lat'].values[0]
            dev_lyst = self.query_dev_id(lon,lat)
            time_lyst = []
            city_lyst = []
            abs_lyst = []
            start_time = '2018-11-18 00:00:00'
            end_time = '2018-12-18 00:00:00'
            datas_df = self.dao.query_adj_data_by_device_list(dev_lyst, start_time, end_time)
            for time in self.time_lyst:
                data_df = datas_df.copy()
                data_time_df = data_df[data_df['ADJ_TIME'] == time]
                data_time_df.to_csv('css.csv')
                value_lyst = list(data_time_df[data_time_df['VAR_TYPE_ID'] == 1]['ADJ_VALUE'].values)
                mean = np.mean(value_lyst)
                city_lyst.append(mean)
                time_lyst.append(time)
                global_lyst.append(mean)
            root_lyst = list(self.root_df[city].values)
            for i in range(len(root_lyst)):
                global_root_lyst.append(root_lyst[i])
                if root_lyst[i] and city_lyst[i]:
                    res_value = abs(root_lyst[i]-city_lyst[i])/root_lyst[i]
                else:
                    res_value = np.nan
                abs_lyst.append(res_value)
            dict = {'时间':time_lyst,city:city_lyst,city+'_root':root_lyst,'相对偏差':abs_lyst}
            city_df = pd.DataFrame(dict)
            self.draw_picture(city_df, city)
            for i in range(city_df.index.max()):
                # 过滤条件设置
                if any([city_df.loc[i, city+'_root'] < 30 or not city_df.loc[i, city] or not city_df.loc[i, city+'_root'] ]):
                    print('删除异常值 %s 行数据' % i)
                    city_df.drop([i], inplace=True)
            city_df.dropna(axis=0, how='any', inplace=True)
            city_df.to_csv(city + '.csv')
            self.draw_csv(city_df,city)
        # global_dict={'adj_value':global_lyst,'root':global_root_lyst}
        # ana_df = pd.DataFrame(global_dict)


    def draw_picture(self,city_df,city=None):
        plt.figure(figsize=(12, 8))
        plt.tick_params(labelsize=12)
        plt.rcParams['font.sans-serif'] = 'SimHei'
        plt.rcParams['axes.unicode_minus'] = False
        dfs = city_df.copy().dropna(axis=0,how='any')
        # 取出身高和体重两列数据
        if city:
            y = dfs[city+'_root']
            x = dfs[city]
        plt.scatter(x, y)
        # x，y取值范围设置
        # 可以过滤掉一部分脏数据
        plt.xlim(0, 200)
        plt.ylim(0, 200)
        plt.axis()
        # 设置title和x，y轴的label
        if city :
            plt.title(city+"浓度对比散点图")
            plt.ylabel("目标设备测量值")
            plt.xlabel("己方设备测量值")
        # 保存图片到指定路径
            plt.savefig("./picture/"+city+".png")
        # 展示图片 *必加
        # plt.show()

    def draw_csv(self,city_df,city):
        lyst0 = list(city_df[city_df['相对偏差']<=0.1].values)
        lyst1 = list(city_df[city_df['相对偏差']<=0.2 ].values)
        lyst2 = list(city_df[city_df['相对偏差']<=0.3].values)
        lyst3 = list(city_df[city_df['相对偏差']<=0.4].values)
        lyst4 = list(city_df[city_df['相对偏差']<=0.5].values)
        lyst5 = list(city_df[city_df['相对偏差']<=0.6].values)
        lyst6 = list(city_df[city_df['相对偏差']<=0.7].values)
        lyst7 = list(city_df[city_df['相对偏差']<=0.8].values)
        lyst8 = list(city_df[city_df['相对偏差']<=0.9].values)
        lyst9 = list(city_df[city_df['相对偏差']<=1.0 ].values)
        lyst10 = list(city_df[city_df['相对偏差']>1.0].values)
        sum = len(city_df)
        s1 = len(lyst0)/sum
        s2 = len(lyst1)/sum
        s3 = len(lyst2)/sum
        s4 = len(lyst3)/sum
        s5 = len(lyst4)/sum
        s6 = len(lyst5)/sum
        s7 = len(lyst6)/sum
        s8 = len(lyst7)/sum
        s9 = len(lyst8)/sum
        s10 = len(lyst9)/sum
        s11 = len(lyst10)/sum
        lysta = ['<=10%','10%-20%','20%-30%','30%-40%','40%-50%','50%-60%','60%-70%','70%-80%','80%-90%','90%-100%','>100%']
        lystb = [s1,s2-s1,s3-s2,s4-s3,s5-s4,s6-s5,s7-s6,s8-s7,s9-s8,s10-s9,s11]
        dict = {'区间':lysta,'占比':lystb}
        res_df = pd.DataFrame(dict)

        num_list = res_df.占比
        x = list(range(len(num_list)))
        width = 0.3
        fig, ax = plt.subplots(figsize=(14, 8))
        b = ax.bar(x,res_df['占比'],width,tick_label=res_df['区间'] )
        for i in b:
            h = i.get_height()
            print(h)
            ax.text(i.get_x(),h,'%.2f%%'%float(h*100))
        plt.xticks(rotation=0)
        plt.tick_params(labelsize=12)
        plt.title(city + "按相对偏差分桶的占比图")
        plt.xlabel("设备占比")
        plt.ylabel("相对偏差分桶")
        plt.tight_layout()
        plt.savefig('./picture/' + city + '按相对偏差分桶的占比图')
        # plt.show()
        plt.close('all')




        # sn = 0
        # for j in range(30, (len(res_df) // 30 + 1) * 30 + 1, 30):
        #     var = res_df[sn:j]
        #     sn = j
        #     name_list = var.区间
        #     num_list = var.占比
        #     plt.figure(figsize=(14, 8))
        #     x = list(range(len(num_list)))
        #     total_width, n = 1.2, 2
        #     width = total_width / n
        #     # fig, ax = plt.subplots()
        #     # b = ax.barh(x, num_list, width=width, fc='b',tick_label = name_list, color='#6699CC')
        #     b = plt.bar(x, num_list, width=width, fc='b',tick_label = name_list)
        #     # for rect in b:
        #     #     h = rect.get_height()
        #     #     ax.text(b.get_x()+b.get_width()/2, h, '%d' % int(h), ha='center', va='top')
        #     plt.xticks(rotation=0)
        #     plt.tick_params(labelsize=12)
        #     plt.title(city+"按相对偏差分桶的占比图")
        #     plt.xlabel("设备占比")
        #     plt.ylabel("相对偏差分桶")
        #     plt.tight_layout()
        #     plt.savefig('./picture/'+ city+'按相对偏差分桶的占比图')
        #     plt.close('all')

    def get_root_df(self):
        df = pd.read_table('copy.csv', encoding='ANSI', sep=',')
        self.time_lyst = list(df['时间'].values)
        return df

if __name__ == '__main__':
    hour = '2018-12-17 10:00:00'
    ana = Analyze(hour)
    # ana.get_root_df()
    # res = ana.query_dev_id(116.18933,39.802111)
    res = ana.handle()
    print(res)
    # df = pd .read_table('copy.csv',encoding='ANSI', sep=',')
    # print(df)
    # print(df['时间'].values[0])
    # df.to_csv('ass.csv',encoding='utf-8')


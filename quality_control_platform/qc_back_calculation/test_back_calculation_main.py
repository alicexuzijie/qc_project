# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181126
@filename:test_back_calculation_main.py
@author:Yang
"""
from qc_back_calculation.back_calculation_main import BackCalculation
import time
import pandas as pd

def bc_org_adjust_by_city(hour, city_id):
    bc = BackCalculation(hour)
    #按城市进行回算 org和adjust
    bc.execute_back_calculation(hour, city_id, is_for_org=True)

def bc_adjust_by_city(hour, city_id):
    bc = BackCalculation(hour)
    # 按城市进行回算 只回算adjust
    bc.execute_back_calculation(hour, city_id)

def bc_org_adjust_by_devices(hour, city_id, device_list):
    bc = BackCalculation(hour)
    #按照 城市的设备编号 进行回算 回算 org和adjust
    bc.execute_back_calculation(hour, city_id, dev_list=device_list)


time1 = time.time()
hour = '2018-12-12 10:00:00'
city_id = [1]
devices_df = pd.read_csv('beijing_.csv')
dev_list = devices_df['DEV_ID'].unique().tolist()

#按照 城市的设备编号 进行回算
# bc.execute_back_calculation(hour, city_id, dev_list=dev_list)
#按照想要回算 城市的设备编号 和var进行回算
# bc.execute_back_calculation(hour, city_id, var_names=['PM25','PM10'], dev_list=dev_list)
#按城市进行回算 cap->org->adjust
# bc.execute_back_calculation(hour, city_id, is_for_org=True)
bc_org_adjust_by_devices(hour, city_id, device_list=dev_list)
# bc_adjust_by_city(hour, city_id)
time2 = time.time()
print("补算城市{}，耗时{}".format(city_id, time2 - time1))
# -*- coding:utf-8 -*-

import time
import pandas as pd
import datetime
import sys
import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
from utility import time_utility as tu
from qc_back_calculation.back_calculation_main import BackCalculation


def back_calculate(city_id_list, start_time, end_time, is_for_org=False):
    time1 = time.time()
    start_datatime = tu.time_str_to_datetime(start_time)
    end_datatime = tu.time_str_to_datetime(end_time)
    n_hours = int((end_datatime - start_datatime).seconds/3600)
    for city_id in city_id_list:
        for i in range(0, n_hours+1):
            hour = tu.datetime_n_hours_after_string(start_datatime, i)
            print("开始回算城市:%s。时间:%s"%(city_id,hour))
            if is_for_org:
                bc = BackCalculation(hour)
                bc.execute_back_calculation(hour, city_id, is_for_org=True)
                print('')
            else:
                bc = BackCalculation(hour)
                bc.execute_back_calculation(hour, city_id)
                print('')
    time2 = time.time()
    print("补算城市{}，耗时{}".format(city_id, time2 - time1))


#back_calculate([[295]], '2018-12-06 10:00:00', '2018-12-06 12:00:00')
param_city = sys.argv[1].split(',')
city_list = list(map(lambda x:[int(x)], param_city))
back_calculate(city_list, sys.argv[2], sys.argv[3])


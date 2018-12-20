# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181218
@filename:main_by_minute.py
@author:Yang
"""
import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
import datetime
import time
from quality_control.quality_control_main_by_minute import QualityControlRoutineByMinute
if __name__ == '__main__':
    hour_minute = (datetime.datetime.now() - datetime.timedelta(hours=0)).strftime('%Y-%m-%d %H:%M:%S')
    print(hour_minute)
    city_id = [218]
    print(city_id)
    qc_by_minute = QualityControlRoutineByMinute(hour_minute)
    qc_by_minute.execute_quality_control_by_minute(hour_minute, city_id)

# -*- coding:utf-8 -*-
# Created by yang
"""
@create time :20181213
@filename:file_utility.py
@author:Yang
"""
from utility import time_utility as tu

def get_model_folder_name_by_date(hour):
    """
    拼接模型存储文件名, 一天的文件夹
    :param hour:
    :return:
    """
    date_str = tu.extract_number_from_datetime_str(hour[0:10])
    return "models_{}".format(date_str)


def get_model_folder_name_by_hour(hour):
    """
    拼接模型存储文件名， 一个小时的文件夹
    :param hour:
    :return:
    """
    hour_str = tu.extract_number_from_datetime_str(hour[11:13])
    return "models_{}".format(hour_str)

def get_save_path(hour_minute):
    """
    一天,下面是所有的csv
    :param hour_minute:
    :return:
    """
    date_str = tu.extract_number_from_datetime_str(hour_minute[0:10])
    date_str_path = "adjust_by_minute_{}".format(date_str)
    # hour_str = tu.extract_number_from_datetime_str(hour_minute[11:13])
    # hour_str_path = "adjust_by_minute_{}".format(hour_str)
    # dir = '{}'.format(date_str_path)
    return date_str_path

def get_csv_name(hour_minute):
    csv_name = tu.extract_number_from_datetime_str(hour_minute)
    return 'adjust_{}.csv'.format(csv_name)

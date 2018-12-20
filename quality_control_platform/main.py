# encoding = utf-8

import argparse


def quality_control_all(is_proc_capture):
    """
    对所有sensor_info中的设备进行质控

    Args:
        is_proc_capture: 是否需要重算capture到org的过程
    """
    pass


def quality_control_by_city(city_list, is_proc_capture):
    """
    对某些城市的所有sensor_info中的设备进行质控

    Args:
        city_list: 城市列表
        is_proc_capture: 是否需要重算该城市capture到org的过程
    """
    pass


def model_trans_by_devices(device_list, is_proc_capture):
    """
    对设备列表中的设备进行模型传递，注意对于给定设备清单，不会重算给定设备对应的质控点的数据

    Args:
        device_list: 设备列表
        is_proc_capture: 是否需要重算该城市capture到org的过程
    """

    pass


def capture_to_org_all():
    """
    为所有设备处理capture_to_org的数据
    """
    pass


if __name__ == '__main__':
    capture_to_org_all()

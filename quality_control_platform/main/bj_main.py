# encoding = utf-8

import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
from aggregate_capture.agg_capture import AggregateCapture
from dao.mysql_impl import DataOperationsByMysql,manager_dao
from config.qc_config import QualityControlConfig
from aux_entities.vargroup_channels import VargroupChannels
from quality_control.quality_control_main import QualityControlRoutine
from log import log
import datetime
import time
from multiprocessing import Pool
from multiprocessing import Pool,Manager

logger = log.log_demo()


def quality_control_all(is_proc_capture):
    """
    对所有sensor_info中的设备进行质控

    Args:
        is_proc_capture: 是否需要重算capture到org的过程
    """
    pass


def quality_control_by_city(city_list, hour, dao):
    """
    对某些城市的所有sensor_info中的设备进行质控

    Args:
        city_list: 城市列表
        is_proc_capture: 是否需要重算该城市capture到org的过程
    """
    print('Enter prepare data at city {}'.format(city_list))
    qc_routine = QualityControlRoutine(dao)
    start_quality_control = time.time()
    qc_routine.obtain_adjust_data(city_list, hour)
    # qc_routine.adjust_df_full.to_csv('adjust_df_full_{}.csv'.format(city_list))
    # qc_routine.interpolate_df.to_csv('interpolate_df_{}.csv'.format(city_list))
    # qc_routine.site_interpolate_df.to_csv('site_interpolate_{}.csv'.format(city_list))
    end_control = time.time()
    run_time = end_control-start_quality_control
    logger.info('quality_control_by_city函数花费时间%s,城市：%s' %(run_time,city_list))


def model_trans_by_devices(device_list, is_proc_capture):
    """
    对设备列表中的设备进行模型传递，注意对于给定设备清单，不会重算给定设备对应的质控点的数据

    Args:
        device_list: 设备列表
        is_proc_capture: 是否需要重算该城市capture到org的过程
    """

    pass


def capture_to_org_all(hour, dev_df, my_config, dao, vg_c, models):
    """
    为所有设备处理capture_to_org的数据
    """
    start_dfs = time.time()
    # 按照设备清单获得相关的capture dataframe
    dfs = dao.query_capture_data_by_hour(hour, dev_df)
    end_dfs = time.time()
    logger.info('按照设备清单获得相关的capture dataframe花费时间为%s' % (end_dfs - start_dfs))
    # 实例化
    ac = AggregateCapture(my_config, dao, dfs, vg_c, models)
    start_capture_to_org = time.time()
    ac.capture_to_org(hour)
    end_capture_to_org = time.time()
    logger.info('capture_to_org函数执行花费%s' % (end_capture_to_org - start_capture_to_org))


def new_aux_objects(hour):
    # 加载单例配置对象

    start_config = time.time()
    my_config = QualityControlConfig()
    end_config = time.time()
    logger.info('主函数加载单例配置对象QualityControlConfig类初始化时间为%s' % (end_config - start_config))

    start_dao = time.time()
    # dao = DataOperationsByMysql(my_config,hour)
    manager = manager_dao()
    dao = manager.DataOperationsByMysql(my_config,hour)
    end_dao = time.time()
    logger.info('主函数生成数据操作层对象DataOperationsByMysql类初始化时间为%s' % (end_dao - start_dao))

    # 获得vargroup相关数据
    start_vg_c = time.time()
    channel_df = dao.query_channels()
    aq_dict = dao.query_aq_type_in_dict()
    vg_c = VargroupChannels(channel_df, aq_dict)
    end_vg_c = time.time()
    logger.info('主函数获得vargroup相关数据VargroupChannels类初始化时间为%s' % (end_vg_c - start_vg_c))

    # 获得模型相关数据
    start_models = time.time()
    models = dao.query_consistency_model()
    end_models = time.time()
    logger.info('主函数获得模型相关数据query_consistency_model类初始化时间为%s' % (end_models - start_models))

    return my_config, dao, vg_c, models


if __name__ == '__main__':
    start_project = time.time()
    hour = (datetime.datetime.now() - datetime.timedelta(hours=0)).strftime('%Y-%m-%d %H:00:00')
    #hour = '2018-12-15 02:00:00'
    print(hour)
    #加载配置项
    my_config, dao, vg_c, models = new_aux_objects(hour)
    end_config = time.time()
    logger.info('加载完全部配置项花费时间%s' % (end_config - start_project))
    #获取所有设备信息
    dev_df = dao.query_active_devices()
    end_dev_df = time.time()
    logger.info('获取所有设备信息耗时%s' % (end_dev_df - end_config))
   # capture_to_org_all(hour, dev_df, my_config, dao, vg_c, models)
    city_list = [[1]]
   # pool = Pool(12)
    for city in city_list:
        print(city)
        quality_control_by_city(city, hour, dao)
        #pool.apply_async(quality_control_by_city, args=(city, hour, dao))
    #pool.close()
    #pool.join()
    end_quality_control_by_city = time.time()
    # logger.info('quality_control_by_city函数执行花费%s' % (end_quality_control_by_city - end_capture_to_org_all))
    logger.info('整个项目运行耗时%s' % (end_quality_control_by_city - start_project))

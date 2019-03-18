import time
import common
from quality_control.quality_control_main import QualityControlRoutine
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig


def qc():
    t_1 = time.time()
    print('Enter prepare data')
    config = QualityControlConfig()
    # hour = '2018-11-26 15:00:00'
    hour = '2018-12-18 21:00:00'
    dao = DataOperationsByMysql(config, hour)
    t_2 = time.time()
    # adjust_df,interpolate_df = qc_routine.execute_train_transmission_by_city([1],'2018-09-05 02:00:00')

    # city_list = [[2], [197, 492], [149], [201], [202], [203], [204], [205], [206], [208],
    #              [210], [212], [213], [229], [231], [232], [235], [238], [239], [245], [291], [296], [297],
    #              [298], [303], [306], [307], [308], [662], [771]]

    for cityid in [[1]]:
        print("城市：{}".format(cityid))
        t_3 = time.time()
        qc_routine = QualityControlRoutine(dao)
        qc_routine.obtain_adjust_data(cityid,hour)
        t_4 = time.time()
        print('城市{} 需要的时间是：{}'.format(cityid,t_4-t_3))
    t_5 = time.time()
    print('Total execution time of QC and transmission is {} seconds'.format(t_5 - t_1))
qc()
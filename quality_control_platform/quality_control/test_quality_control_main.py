
import common

from quality_control_main import QualityControlRoutine
from dao.mysql_impl import DataOperationsByMysql
from config.qc_config import QualityControlConfig


def test_prepare_data():
    print('Enter prepare data')

    dao = DataOperationsByMysql()
    config = QualityControlConfig()
    qc_routine = QualityControlRoutine(dao, [206],  config, None, '2018-10-25 10:00:00')

    qc_routine.prepare_data()

test_prepare_data()
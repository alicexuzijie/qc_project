import unittest
from qc_config import QualityControlConfig

class ConfigTestCase(unittest.TestCase):
    def test_get_config_global_data(self):
        config = QualityControlConfig()
        config_data = config.get_config_global_data('model_list')
        self.assertEqual(config_data,['lr', 'svr', 'scale_by_ratio', 'xgboost', 'k_means'])

    def test_get_config_city_data(self):
        config = QualityControlConfig()
        config_data = config.get_config_city_data('is_aqi',1)
        self.assertEqual(config_data, True)

    def test_get_config_var_data(self):
        config = QualityControlConfig()
        config_data = config.get_config_var_data('effective_model_list','CO')
        self.assertEqual(config_data, ['lr'])


def test_get_config_global_list():
    config = QualityControlConfig()
    res = config.get_config_global_list()
    print(res)


def test_get_config_city_list():
    config = QualityControlConfig()
    res = config.get_config_city_list()
    print(res)


def test_get_config_var_list():
    config = QualityControlConfig()
    res = config.get_config_var_list()
    print(res)


def test_get_config_global_data():
    config = QualityControlConfig()
    res = config.get_config_global_data('model_list')
    print(res)


def test_get_config_city_data():
    config = QualityControlConfig()
    res = config.get_config_city_data('is_aqi', 1)
    print(res)


def test_get_config_var_data():
    config = QualityControlConfig()
    res = config.get_config_var_data('effective_model_list','CO')
    print(res)


if __name__ == "__main__":
    test_get_config_global_list()
    test_get_config_city_list()
    test_get_config_var_list()
    test_get_config_global_data()
    test_get_config_city_data()
    test_get_config_var_data()
    unittest.main()
import pandas as pd

import common
import feature_generator as feature_gen
import config.qc_config as qc_config

pd.set_option('max_columns', 20)

def gen_test_df():
    df = pd.DataFrame({'DEV_ID': ['YSRD1', 'YSRD2', 'YSRD3'],'SITE_PM25': [20, 19, 35], 'SITE_PM10': [30, 28, 67], 'SITE_NO2':[55, 62, 77],'PM25':[22, 23, 30], 'PM10':[28, 34, 70], 'NO2':[86, 75, 98], 'HUMIDITY':[50, 55, 56], 'TIMESTAMP':[23, 22, 10],'VARGROUP_ID':['va1','va1','va2']})
    print('================DataFrame for test=============')
    print(df.head())
    return df


def test_feature_generator():
    df = gen_test_df()
    config = qc_config.QualityControlConfig()
    f_gen = feature_gen.FeatureManager(config)
    print('=================Test to get features=============')
    X = f_gen.get_prepared_data_for_prediction(df, 'PM25')
    print(X)

    X = f_gen.get_prepared_data_for_prediction(df, 'PM10')
    print(X)

    print('=================Test to get y and features=============')
    y, X = f_gen.get_prepared_data_for_train(df, 'PM25')
    print(y)
    print(X)

    y, X = f_gen.get_prepared_data_for_train(df, 'PM10')
    print(y)
    print(X)

    print('=================Test to get feature column names=============')
    print(f_gen.get_feature_names_by_var('PM10'))

test_feature_generator()
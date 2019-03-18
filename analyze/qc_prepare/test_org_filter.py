import pandas as pd

import common

import org_filter
import dao.mysql_impl as mysql_impl
import config.qc_config as qc_config

pd.set_option('display.max_columns', 500) 


def test_org_filter():
    config = qc_config.QualityControlConfig()
    dao = mysql_impl.DataOperationsByMysql()
    org_f = org_filter.OrgFilter(config)

    print('begin query org data')

    org_df = dao.query_qc_dev_org_data_by_city([1], '2018-09-10 05:00:00')
    site_df = dao.query_site_data_by_city([1], '2018-09-10 05:00:00')
    org_site_df = org_df.merge(site_df, on=['TIMESTAMP', 'SITE_ID'], how='left')

    print(org_site_df.head())

    print('end query org data')

    pollutants = ['PM25', 'PM10', 'TSP', 'SO2', 'CO', 'NO2', 'O3', 'TVOC']

    for p in pollutants:
        print('===============var: {}==================='.format(p))
        print('--------before few entry filter--------')
        print(org_df.head())
        var_df = org_f.few_entry_filter(org_site_df, p)

        if var_df.empty:
            continue

        print('--------before column filter--------')
        print(var_df.head())
        var_df = org_f.column_filter(var_df, p)

        print('--------before nan filter--------')
        print(var_df.head())
        var_df = org_f.nan_filter(var_df)

        print('--------before specific filter--------')
        print(var_df.head())
        var_df = org_f.var_specific_filter(var_df, p)

        print('--------before drop by devices--------')
        print(var_df.head())
        print(var_df['DEV_ID'].values)
        print('# of devices before removal: {}'.format(var_df['DEV_ID'].nunique()))
        var_df = org_f.not_enough_device_entry_filter(var_df)
        print('# of devices after removal: {}'.format(var_df['DEV_ID'].nunique()))

        print('--------final output--------')
        print(var_df.head())

test_org_filter()


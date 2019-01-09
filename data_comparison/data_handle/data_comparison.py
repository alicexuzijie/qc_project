import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
from utility import  mysql_connector
import pandas as pd
import datetime


def sql_handle(table,kind, start_hour, end_hour, var='',dev_lyst=''):
    if dev_lyst and var:
        sql_str = "and VAR_TYPE_ID=" + str(var) + " AND DEV_ID IN (" + dev_lyst+")"
    elif dev_lyst:
        sql_str = " AND DEV_ID IN (" + dev_lyst+")"
    elif var:
        sql_str = "and VAR_TYPE_ID="+str(var)
    else:
        sql_str = ''
    sql = "SELECT COUNT(VAR_TYPE_ID),ADJ_TIME FROM {} WHERE ADJ_TIME >= '{}' and ADJ_TIME<= '{}' {} group by ADJ_TIME".format(table,start_hour,end_hour,sql_str)
    sql_data = mysql_connector.mysql_export_data_to_df(sql, kind)
    return sql_data


def sql_handle_cityid(kind, cityid):
    sql = "SELECT SENSOR_ID FROM MEASURE_POINT INNER JOIN SENSOR_INFO ON MEASURE_POINT.ID=SENSOR_INFO.MEASURE_POINT_ID WHERE MEASURE_POINT.CITYID='"+str(cityid)+"'"
    sql_data = mysql_connector.mysql_export_data_to_df(sql, kind)
    return sql_data


def var_handle(table,kind,start_hour,end_hour,res_df,var=''):
    if not sql_handle(table,kind,start_hour,end_hour,var).empty:
        res = sql_handle(table,kind, start_hour,end_hour,var)
    else:
        res = pd.DataFrame([],columns=['ADJ_TIME'])
    if kind == 'MYSQL-SENSOR1':
        str_kind = '生产库'
    else:
        str_kind = '测试机'
    if var == '1':
        str_var = 'pm25'
    elif var == '2':
        str_var = 'pm10'
    elif var == '3':
        str_var = 'so2'
    elif var == '4':
        str_var = 'co'
    elif var == '5':
        str_var = 'no2'
    elif var == '6':
        str_var = 'o3'
    elif var == '10':
        str_var = 'tsp'
    else:
        str_var = '总条数'
    res.rename(columns={'COUNT(VAR_TYPE_ID)':str_kind+str_var}, inplace=True)
    res_df = pd.merge(res_df,res, how='outer', on=['ADJ_TIME'])
    return res_df


def data_handle_var(table,start_hour,end_hour,filehour):
    production = 'MYSQL-SENSOR1'
    test = 'MYSQL-SENSOR1A'
    res_df = pd.DataFrame([],columns=['ADJ_TIME'])
    time = sql_handle(table, production, start_hour, end_hour)['ADJ_TIME'].values
    res_df = var_handle(table,production,start_hour,end_hour,res_df)
    res_df = var_handle(table,test,start_hour,end_hour,res_df)
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='1')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='1')
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='2')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='2')
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='3')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='3')
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='4')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='4')
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='5')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='5')
    res_df = var_handle(table,production,start_hour,end_hour,res_df,var='6')
    res_df = var_handle(table,test,start_hour,end_hour,res_df,var='6')
    res_df = var_handle(table, production, start_hour, end_hour, res_df, var='10')
    res_df = var_handle(table, test, start_hour, end_hour, res_df, var='10')
    city_filename = 'city_var_global_' + filehour + '.csv'
    res_df.to_csv('../csv_city/' + city_filename, encoding='utf_8_sig')


def var_handle_city(table,test,production,start_hour,end_hour,test_dev_list,pro_dev_list,city,city_df,var=''):
    if var == '1':
        str_var = 'pm25'
    elif var == '2':
        str_var = 'pm10'
    elif var == '3':
        str_var = 'so2'
    elif var == '4':
        str_var = 'co'
    elif var == '5':
        str_var = 'no2'
    elif var == '6':
        str_var = 'o3'
    elif var == '10':
        str_var = 'tsp'
    else:
        str_var = '总条数'
    df_test = sql_handle(table, test, start_hour, end_hour, dev_lyst=test_dev_list)
    df_test.rename(columns={'COUNT(VAR_TYPE_ID)': '测试库'+str_var+'_' + str(city)}, inplace=True)
    df_pro = sql_handle(table, production, start_hour, end_hour, dev_lyst=pro_dev_list)
    df_pro.rename(columns={'COUNT(VAR_TYPE_ID)': '生产库'+str_var+'_' + str(city)}, inplace=True)
    if not (sql_handle(table, test, start_hour, end_hour, dev_lyst=test_dev_list).empty and
            sql_handle(table,production,start_hour,end_hour,dev_lyst=pro_dev_list).empty):
        df_res = pd.merge(df_test, df_pro, how='outer', on=['ADJ_TIME'])
    else:
        df_res = pd.DataFrame([],columns=['ADJ_TIME'])
    city_df = pd.merge(city_df, df_res, how='outer', on=['ADJ_TIME'])
    return city_df


def data_handle_city(city_list,start_hour,end_hour,table,filehour):
    production = 'MYSQL-SENSOR1'
    test = 'MYSQL-SENSOR1A'
    city_df = pd.DataFrame([],columns=['ADJ_TIME'])
    # filehour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H_00_00')
    for city in city_list:
        test_dev_list = str(sql_handle_cityid(test,city)['SENSOR_ID'].tolist())[1:-1]
        pro_dev_list = str(sql_handle_cityid(production,city)['SENSOR_ID'].tolist())[1:-1]

        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df)
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='1')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='2')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='3')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='4')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='5')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='6')
        city_df=var_handle_city(table, test, production, start_hour, end_hour, test_dev_list, pro_dev_list, city, city_df,var='10')
        print(city)
    city_filename = 'city_var_' + filehour + '.csv'
    city_df.to_csv('../csv_city/' + city_filename, encoding='utf_8_sig')


if __name__ == '__main__':
    city_list = [1, 2, 149, 197, 198, 201, 202, 203, 204, 205, 206, 208, 210, 212, 213, 229, 231, 232, 235, 238,
                 239,245, 291, 296, 297, 298, 303, 306, 307, 308, 662, 771, 492, 493, 502,295,314]
    # city_list = [1]

    # hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
    start_hour = '2018-12-09 00:00:00'
    end_hour = '2018-12-09 23:00:00'
    table = 'DEVICE_ADJUST_VALUE_BYHOUR_201812'
    filehour = '2018_12_09'
    data_handle_var(table,start_hour,end_hour,filehour)
    # data_handle_city(city_list,start_hour,end_hour,table,filehour)
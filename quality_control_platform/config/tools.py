import sys

import common

from utility import utility
from utility import mysql_connector
from error_demo.error_code import *
from log import log
logger = log.log_demo()


def singleton(cls, *args, **kwargs):
    """
    本函数为单例装饰器函数
    :param cls: 作用的类作为参数传递进去
    :param args:元组类型的不定长参数
    :param kwargs:字典类型的不定长参数
    :return:外层函数返回内层函数，内层函数返回单例对象
    """
    instance = {}

    def _instance():
        if cls not in instance:
            instance[cls] = cls(*args, *kwargs)
        return instance[cls]
    return _instance


def data_type_conversion(df):
    """
    本函数主要功能为将dataframe中value列的值根据type属性进行转换
    :param df: 传递进来的dataframe数据
    :return: 对应数据类型类型的返回值
    """
    df_type = df['parse_type']
    if df_type is None:
        raise NullFieldError('parse_type字段内容为空')
    else:
        data = df['parse_type'].values[0].strip()
        df_data = df.copy().reset_index(drop=True)
        if data == 'int':
            res=data_handle(df_data, 'int')
        elif data == 'float':
            res = data_handle(df_data,'float')
        elif data == 'boolean':
            res=boolean_handle(df_data)
        elif data == 'list<string>':
            res=list_handle(df_data,'list<string>')
        elif data == 'list<int>':
            res = list_handle(df_data, 'list<int>')
        elif data == 'string':
            res = df_data['value'].values[0].strip()
        else:
            raise SqlValueError('parse_type字段内容不在合法范围内')
    return res


def data_handle(df,type):
    """
    将type为float或int的value转换为对应类型数值
    :param df: 待处理的数据
    :param type: 数据类型
    :return: 对应类型的数据
    """
    df.value = df.value.astype(type)
    res = df['value'].values[0]
    return res


def boolean_handle(df):
    """
    将type为boolean的value转换为对应类型数值
    :param df: 待处理的数据
    :return: 对应类型的数据
    """
    for i in range(len(df['parse_type'])):
        if df['value'].values[i].strip() == 'FALSE':
            df['value'].values[i] = ''
        elif df['value'].values[i].strip() == 'TRUE':
            df['value'].values[i] = 'TRUE'
        else:
            raise SqlValueError('value字段内容不在合法范围内')
    df.value = df.value.astype(bool)
    res = df['value'].values[0]
    return res


def list_handle(df,type):
    """
    将type为list的value转换为对应类型数值
    :param df: 待处理的数据
    :param type: 数据类型
    :return: 对应类型的数据
    """
    df_str = df['value'].values[0]
    res = df_str.strip().split(",")
    if type == 'list<int>':
        for i in range(len(res)):
            res[i] = int(res[i])
    elif type == 'list<string>':
        res = res
    else:
        raise SqlValueError('parse_type字段内容不在合法范围内')
    return res


def sql_handle(table_sub,table_sup,item=''):
    """
    数据库操作函数，通过SQL语句调用mysql_export_data_to_df函数，获取对应的df数据
    :param table_sub: 查询配置项的数值表
    :param table_sup: 查询配置项的配置项表
    :param item: 附加参数，例如：city_id,var_type
    :return: 返回对应的配置项df数据
    """
    if item:
        sql = 'SELECT config_item,value,parse_type,' + item + ' from ' + table_sub + \
              ' INNER JOIN ' + table_sup + ' on ' + table_sub + '.config_item_id = '\
              + table_sup + '.id'
    else:
        sql = 'SELECT config_item,value,parse_type from ' + table_sub + \
              ' INNER JOIN ' + table_sup + ' on ' + table_sub + '.config_item_id = ' \
              + table_sup + '.id'
    sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1A')
    if sql_data is None:
        raise NoneDfError('未返回df数据，可能数据库连接有异常')
    elif sql_data.empty:
        raise EmptyDfError('查询数据库获取的df数据为空')
    return sql_data


def sql_data_handle(table):
    """
    数据库操作函数，通过SQL语句调用mysql_export_data_to_df函数，获取对应的df数据
    :param table_sub: 查询配置项的数值表
    :param table_sup: 查询配置项的配置项表
    :return: 返回对应的配置项df数据
    """
    sql = 'SELECT CONFIG_ITEM,`DESCRIPTION` from ' + table
    sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1A')
    if sql_data is None:
        raise NoneDfError('未返回df数据，可能数据库连接有异常')
    elif sql_data.empty:
        raise EmptyDfError('查询数据库获取的df数据为空')
    return sql_data


def var_name_handle(table):
    """
    数据库操作函数，通过SQL语句调用mysql_export_data_to_df函数，获取对应的df数据
    :param table_sub: 查询配置项的数值表
    :param table_sup: 查询配置项的配置项表
    :return: 返回对应的配置项df数据
    """
    sql = 'SELECT AQ_TYPE,NAME from ' + table
    sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1A')
    if sql_data is None:
        raise NoneDfError('未返回df数据，可能数据库连接有异常')
    elif sql_data.empty:
        raise EmptyDfError('查询数据库获取的df数据为空')
    return sql_data


def sql_data_to_dict(table_sub,table_sup,item='',sql_var=None):
    """
    将sql_handle函数获取的df数据转换为config_item为key,对应value的字典类型
    :param table_sub: 查询配置项的数值表(为了调用sql_handle函数）
    :param table_sup: 查询配置项的配置项表(为了调用sql_handle函数）
    :param item: 附加参数，例如：city_id,var_type(为了调用sql_handle函数）
    :return: 返回对应的配置项信息，类型为字典
    """
    sql_data=sql_handle(table_sub,table_sup,item)
    sql_dict={}
    # 遍历config_item将其作为key放在外层字典中
    for i in range(len(sql_data['config_item'])):
        sql_data_key=sql_data['config_item'].values[i]
        # 如果是城市级别或者参数级别的需要传递item生成内层字典
        if item:
            # 判断该config_item是否存在，如果存在跳过本次操作
            if sql_data_key not in sql_dict.keys():
                value_data = {}
                value_data_sql = sql_data[sql_data['config_item'] == sql_data_key]
                # 遍历item将其作为key放在内层字典中
                for j in range(len(value_data_sql)):
                    key=value_data_sql[item].values[j]
                    # 如果是参数级别的需要将参数名称作为key放在内层字典中
                    if item == 'VAR_TYPE':
                        value_data_name = sql_var[sql_var['AQ_TYPE'] == key]['NAME'].values[0]
                        value_data_key = utility.exclude_dot_in_var_name(value_data_name)
                    else:
                        value_data_key = key
                    try:
                        value_data_values = data_type_conversion(value_data_sql[value_data_sql[item] == key])
                    except BaseError as e:
                        e.setdata({'item': sql_data_key, 'key': value_data_key})
                        logger.error('code:%s,name:%s,message:%s,data:%s',
                                         e.code, e.name,
                                         e.message, e.getdata(), exc_info=True)
                    else:
                        value_data[value_data_key] = value_data_values
                sql_dict[sql_data_key] = value_data
        else:
            try:
                value_data = data_type_conversion(sql_data[sql_data['config_item'] == sql_data_key])
            except BaseError as e:
                e.setdata({ 'key': sql_data_key})
                logger.error('code:%s,name:%s,message:%s,data:%s',
                             e.code, e.name,
                             e.message, e.getdata(), exc_info=True)
            else:
                sql_dict[sql_data_key] = value_data
    return sql_dict



def config_data_to_dict(table):
    """
    将sql_handle函数获取的df数据转换为config_item为key,对应value的字典类型
    :param table_sub: 查询配置项的数值表(为了调用sql_handle函数）
    :param table_sup: 查询配置项的配置项表(为了调用sql_handle函数）
    :return: 返回对应的配置项信息，类型为字典
    """
    try:
        sql_data=sql_data_handle(table)
    except BaseError as e:
        e.setdata({'key': table})
        logger.error('code:%s,name:%s,message:%s,data:%s',
                     e.code, e.name,
                     e.message, e.getdata(), exc_info=True)
    else:
        sql_dict={}
        for i in range(len(sql_data['CONFIG_ITEM'])):
            sql_data_key=sql_data['CONFIG_ITEM'].values[i]
            value_data = sql_data['DESCRIPTION'].values[i]
            sql_dict[sql_data_key] = value_data
        return sql_dict


def data_check(data_var,data_global):
    """
    检验data_var是否是data_global子集
    :param data_var:var表
    :param data_global:global表
    """
    for i in data_var:
        if i not in data_global:
            raise ValueRangeError('该参数配置项不在全局配置项范围内')




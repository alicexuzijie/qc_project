import mysql_connector
import time


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
    try:
        data = df['parse_type'].values[0]
        df_data = df.copy().reset_index(drop=True)
    except Exception as err:
        res = None
    else:
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
        else:
            res=df_data['value'].values[0]
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
        if df['value'].values[i] == 'False':
            df['value'].values[i] = ''
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
    res = df_str.split(",")
    if type == 'list<int>':
        for i in range(len(res)):
            res[i] = int(res[i])
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
    return sql_data


def sql_data_handle(table):
    """
    数据库操作函数，通过SQL语句调用mysql_export_data_to_df函数，获取对应的df数据
    :param table_sub: 查询配置项的数值表
    :param table_sup: 查询配置项的配置项表
    :return: 返回对应的配置项df数据
    """
    sql = 'SELECT CONFIG_ITEM,`DESC` from ' + table
    sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1A')
    return sql_data


def var_name_handle(table):
    """
    数据库操作函数，通过SQL语句调用mysql_export_data_to_df函数，获取对应的df数据
    :param table_sub: 查询配置项的数值表
    :param table_sup: 查询配置项的配置项表
    :return: 返回对应的配置项df数据
    """
    sql = 'SELECT AQ_TYPE,NAME from ' + table
    t2=time.time()
    sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1A')
    t3=time.time()
    ts=t3-t2
    print('t=s%',ts)
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
    for i in range(len(sql_data['config_item'])):
        sql_data_key=sql_data['config_item'].values[i]
        if item:
            # print(value_data_sql)
            if sql_data_key not in sql_dict.keys():
                value_data = {}
                value_data_sql = sql_data[sql_data['config_item'] == sql_data_key]
                # print(value_data_sql)
                for j in range(len(value_data_sql)):
                    key=value_data_sql[item].values[j]
                    if item == 'VAR_TYPE':
                        value_data_key = sql_var[sql_var['AQ_TYPE'] == key]['NAME'].values[0]
                    else:
                        value_data_key = key
                    value_data_values=data_type_conversion(value_data_sql[value_data_sql[item] == key])
                    value_data[value_data_key]=value_data_values
                sql_dict[sql_data_key] = value_data
                    # print(value_data)
        else:
            value_data = data_type_conversion(sql_data[sql_data['config_item'] == sql_data_key])
            sql_dict[sql_data_key] = value_data
        # print(sql_dict)
    return sql_dict


def config_data_to_dict(table):
    """
    将sql_handle函数获取的df数据转换为config_item为key,对应value的字典类型
    :param table_sub: 查询配置项的数值表(为了调用sql_handle函数）
    :param table_sup: 查询配置项的配置项表(为了调用sql_handle函数）
    :return: 返回对应的配置项信息，类型为字典
    """
    sql_data=sql_data_handle(table)
    sql_dict={}
    for i in range(len(sql_data['CONFIG_ITEM'])):
        sql_data_key=sql_data['CONFIG_ITEM'].values[i]
        value_data = sql_data['DESC'].values[i]
        sql_dict[sql_data_key] = value_data
    return sql_dict


def data_check(data_var,data_global):
    """
    检验data_var是否是data_global子集
    :param data_var:var表
    :param data_global:global表
    """
    for i in data_var:
        try:
            if i not in data_global:
                raise Exception('{}不在全局中配置项'.format(i))
        except Exception as err:
            print('Caught exception: {}'.format(err))



# encoding = utf-8

import pandas as pd 

def str_list_to_str(cur_list):
    cur_str = "'{}'".format(str(cur_list[0]))
    for i in range(1, len(cur_list)):
        cur_str += ",'"
        cur_str += cur_list[i]
        cur_str += "'"

    return cur_str


def int_list_to_str(cur_list):
    cur_str = "{}".format(cur_list[0])
    for i in range(1, len(cur_list)):
        cur_str += ","
        cur_str += str(cur_list[i])

    return cur_str


def rename_dict_with_prefix(name_list, prefix):
    rename_dict = {}
    for n in name_list:
        rename_dict[n] = '{}_{}'.format(prefix, n)

    return rename_dict


def rename_dict_with_surfix(name_list, surfix):
    rename_dict = {}
    for n in name_list:
        rename_dict[n] = '{}_{}'.format(n, surfix)

    return rename_dict


def get_qa_type_names_by_ids(variable_ids, aq_dict):
    key_list = aq_dict.keys()
    names = []
    for v_id in variable_ids:
        if v_id in key_list:
            names.append(aq_dict[v_id])

    return names


def two_column_df_to_dict(df, key_col, val_col):
    tmp_df = df.copy()
    tmp_df.reset_index(drop=True, inplace=True)

    tmp_dict = {}

    nrows = tmp_df.shape[0]
    for i in range(nrows):
        tmp_dict[tmp_df.ix[i, key_col]] = tmp_df.ix[i, val_col]

    del tmp_df
    return tmp_dict


def exclude_dot_in_var_name(var_name):
    '''
    因为PM2.5的命名不规范，需要将dot去掉
    '''
    var_name = var_name.replace('.', '')
    return var_name


def drop_columns_for_dataframe(df, columns_to_drop):
    '''
    为dataframe drop掉不需要的列，并且返回

    Args:
        df: dataframe to operate on
        columns_to_drop: 需要drop的列名，list<string>
    '''
    print(type(columns_to_drop))
    df = df.drop(columns=columns_to_drop)
    print(df.head())
    return df


def list_to_tuple(list):
    if len(list) == 0:
        print('There is no element contained in this list')
    elif len(list) == 1:
        return '({})'.format(list[0])
    else:
        return tuple(list)
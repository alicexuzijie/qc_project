# encoding = utf-8

import sys
import pandas as pd

import common


class VargroupQCVersions():
    '''
    本类用于管理vargroup channel相关的信息，例如根据var_type_name查channel，
    根据vargroup_id查channel
    '''
    def __init__(self, dao):
        aq_type_dict = dao.query_aq_type_in_dict()
        vargroup_version_df = dao.query_qualitycontrol_version()

        self.qc_versions_by_vargroup_and_var = self.parse_qc_versions_by_vargroup_and_var(vargroup_version_df, aq_type_dict)
        self.qc_vargroup_by_versions_and_var = self.parse_qc_vargroup_by_versions_and_var(vargroup_version_df, aq_type_dict)

    def parse_qc_versions_by_vargroup_and_var(self, vargroup_qc_version_df, aq_type_dict):
        '''
        将vargroup相关的channel信息解析成为一个双层字典
        key_1 = VARGROUP_ID
        key_2 = VAR_ID
        key_3 = channels

        Args:
            vargroup_channel_df: dataframe格式的vargroup channel
            aq_type_dict: var_id与var_name的映射关系

        Return: {VARGROUP_ID:{VAR_ID, CHANNEL_LIST}}
        '''
        vg_ids = vargroup_qc_version_df['VARGROUP_ID'].unique()
        version_dict = {}

        for vg_id in vg_ids:
            vg_version = {}
            var_df = vargroup_qc_version_df[vargroup_qc_version_df['VARGROUP_ID'] == vg_id]
            var_type_ids = var_df['VAR_TYPE_ID'].unique()

            cur_var_qc_version = {}
            for var_id in var_type_ids:
                var_name = aq_type_dict[var_id]
                cur_var_qc_version[var_name] = var_df[var_df['VAR_TYPE_ID'] == var_id]['QUALITYCONTROL_VERSION'].values[0]
                vg_version[var_id] = cur_var_qc_version
            version_dict[vg_id] = cur_var_qc_version

        return version_dict

    def get_qc_version_by_vargroup_and_var(self, vargroup_id, var_name):
        return self.qc_versions_by_vargroup_and_var[vargroup_id][var_name]

    def parse_qc_vargroup_by_versions_and_var(self, vargroup_qc_version_df, aq_type_dict):
        '''
        将vargroup相关的channel信息解析成为一个双层字典
        key_1 = VAR_ID
        key_2 = channels
        key_3 = VARGROUP_ID

        Args:
            vargroup_channel_df: dataframe格式的vargroup channel
            aq_type_dict: var_id与var_name的映射关系

        Return: {VARGROUP_ID:{VAR_ID, CHANNEL_LIST}}
        '''
        vg_ids = vargroup_qc_version_df['VAR_TYPE_ID'].unique()
        version_dict = {}

        for vg_id in vg_ids:
            vg_version = {}
            var_df = vargroup_qc_version_df[vargroup_qc_version_df['VAR_TYPE_ID'] == vg_id]
            var_type_ids = var_df['QUALITYCONTROL_VERSION'].unique()
            var_name = aq_type_dict[vg_id]

            for var_id in var_type_ids:
                group_lyst = list(var_df[var_df['QUALITYCONTROL_VERSION'] == var_id]['VARGROUP_ID'].values)
                vg_version[var_id] = group_lyst
            version_dict[var_name] = vg_version

        return version_dict

    def get_qc_vargroup_by_version_and_var(self, version, var_name):
        '''
        获得给定参数的给定质控版本对应的vargroup_id清单

        Args:
            version: 质控版本
            var_name: 参数名
        Return:
            vargroup_id清单, list
        '''
        return self.qc_vargroup_by_versions_and_var[var_name][int(version)]

    def get_qc_dev_by_version_and_var(self, version, var_name, df_group):
        """
        通过输入版本号以及参数名称获取对应的设备编号
        :param version: 版本号
        :param var_name: 参数名称
        :return: 对应的设备编号
        """
        group_lyst = self.get_qc_vargroup_by_version_and_var(version, var_name)
        dev_lyst = []
        for group_id in group_lyst:
            dev_lyst += list(df_group[df_group['VARGROUP_ID'] == group_id]['DEV_ID'].values)
        return dev_lyst


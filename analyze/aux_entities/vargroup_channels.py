# encoding = utf-8

import sys
import pandas as pd

sys.path.append('../utility')
import utility


class VargroupChannels():
    '''
    本类用于管理vargroup channel相关的信息，例如根据var_type_name查channel，
    根据vargroup_id查channel
    '''
    def __init__(self, vargroup_channel_df, aq_type_dict):
        self.channel_by_vargroup_and_var, self.var_names_by_vargroup = self.parse_channel_by_vargroup_and_var(vargroup_channel_df, aq_type_dict)
        self.channel_by_vargroup = self.parse_channel_by_vargroup()

    def parse_channel_by_vargroup_and_var(self, vargroup_channel_df, aq_type_dict):
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
        vg_ids = vargroup_channel_df['VARGROUP_ID'].unique()
        channel_dict = {}
        var_name_dict = {}

        for vg_id in vg_ids:
            vg_channels = {}
            var_names = []
            var_df = vargroup_channel_df[vargroup_channel_df['VARGROUP_ID'] == vg_id]
            var_type_ids = var_df['VAR_TYPE_ID'].unique()

            for var_id in var_type_ids:
                var_name = aq_type_dict[var_id]
                var_names.append(var_name)
                cur_var_channels = var_df[var_df['VAR_TYPE_ID'] == var_id]['CHANNEL_LIST']
                vg_channels[var_name] = cur_var_channels.values[0].replace(' ', '').split(',')

            channel_dict[vg_id] = vg_channels
            var_name_dict[vg_id] = var_names

        return channel_dict, var_name_dict

    def parse_channel_by_vargroup(self):
        '''
        将一个VARGROUP对应的所有参数的通道进行合并
        例如[PM25_1, PM25_3, PM10_2]
        '''
        channel_dict = {}
        for vg_id in self.channel_by_vargroup_and_var.keys():
            channel_per_vg = []
            for var_id in self.channel_by_vargroup_and_var[vg_id].keys():
                channel_per_vg = channel_per_vg + self.channel_by_vargroup_and_var[vg_id][var_id]
            channel_dict[vg_id] = channel_per_vg
        return channel_dict

    def get_flatten_channels_by_vargroup(self, vargroup_id):
        '''
        返回拍平的出数通道，也就是某个vargroup所有的出数通道
        '''
        return self.channel_by_vargroup[vargroup_id]

    def get_channels_by_vargroup_and_var(self, vargroup_id, var_name):
        return self.channel_by_vargroup_and_var[vargroup_id][var_name]


    def get_var_names_by_vargroup(self, vargroup_id):
        return self.var_names_by_vargroup[vargroup_id]


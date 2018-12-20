# encoding = utf-8

import sys
import pandas as pd

import common

from vargroup_channels import VargroupChannels
from dao.mysql_impl import DataOperationsByMysql


def test_vargroup_channels():
    dao = DataOperationsByMysql()
    aq_type_dict = dao.query_aq_type_in_dict()
    vargroup_channel_df = dao.query_channels()

    vg_channels = VargroupChannels(vargroup_channel_df, aq_type_dict)

    print(vg_channels.channel_by_vargroup_and_var)
    print('\n')
    print(vg_channels.channel_by_vargroup)
    print('\n')
    print(vg_channels.get_var_names_by_vargroup('YSRDAQ07HW'))


test_vargroup_channels()
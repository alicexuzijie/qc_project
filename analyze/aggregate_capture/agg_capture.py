# encoding = utf-8

import pandas as pd
import common
# 导入自开发包
from aggregate_capture import check_functions as chk_f
from aggregate_capture import aggregation_functions as agg_f
from aggregate_capture import consistency_functions as consis_f

from log import log
from error_demo.error_code import *
logger = log.log_demo()

class AggregateCapture():

    def __init__(self, config, dao, dfs, vargroup_channels, consistency_models):
        """
        初始化关键数据
        :param config: 单例配置项对象
        :param dao: 单例数据库连接对象
        :param dfs: 按照{capture_x:Dataframe}组织的表格，key是capture_x, value是capture的数据
        :param vargroup_channels: 是一个VargroupChannel对象，决定不同的设备的污染物类型及不同污染物的出数通道
        :param consistency_models: 一致性模型
        """
        self.dao = dao
        self.config = config
        self.dfs = dfs
        self.vargroup_channels = vargroup_channels
        self.consistency_models = consistency_models
        self.consis_agent = consis_f.ConsistencyAgent(config)
        self.agg_agent = agg_f.AggregationAgent(config)
        self.check_agent = chk_f.CheckAgent(config)

    def capture_to_org(self, hour, is_for_minute=False):
        """
        将capture数据聚合为org数据
        1. 对每种变量的异常数据进行剔除
        2. 处理多个传感器的数据合并（考虑内容：如果三缺一，三缺二，二缺一怎么处理？）
        3. 处理从分钟级到小时级的数据合并
        :return: 不做返回，直接将处理好的org数据入库
        """
        # 获取需要处理的变量，例如温度、湿度实际上也是需要处理的（暂时还没想好怎么organize）
        tables = self.dfs.keys()
        #初始化一个空的字典，装org数据
        org={}
        for cap_x in tables:
            # logger.info('table {} begin....'.format(cap_x))
            df_cap_x = self.dfs[cap_x]

            vg_ids = df_cap_x['VARGROUP_ID'].unique()

            org_vargroupid = pd.DataFrame()
            for vg_id in vg_ids:
                var_names = self.vargroup_channels.get_var_names_by_vargroup(vg_id)
                # logger.info('VARGROUP_ID {} begin...... and the vargroup_id has channels: {}'.format(vg_id,var_names))
                full_df = pd.DataFrame()
                for var_name in var_names:
                    print("正在处理{}表下的{}下的{}污染物......".format(cap_x,vg_id,var_name))
                    channel_header = self.vargroup_channels.get_channels_by_vargroup_and_var(vg_id, var_name)
                    # logger.info('var {} begin.... and the var has channel_header {}'.format(var_name,channel_header))
                    cur_df = df_cap_x[df_cap_x['VARGROUP_ID'] == vg_id][['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID'] + channel_header]

                    # 处理异常数据并输出日志
                    var_df, _, _ = self.check_agent.clean_capture_data(cur_df, var_name, channel_header)

                    # 对clean后的cap数据应用一致性模型
                    if len(var_df.columns) == 3:
                        logger.debug('after clean_capture_data there is no valid data!')
                        continue
                    else:
                        var_df = self.consis_agent.apply_consistency_model(var_df, var_name, channel_header, self.consistency_models)
                        if var_df.empty:
                            logger.debug('after apply_consistency_model there is no valid data!')
                            continue
                        else:
                            # 聚合多通道数据
                            agg_var_df = self.agg_agent.combine_capture_channels(var_df, var_name, channel_header)

                    # # 聚合分钟级数据到小时
                    # ['DEV_ID', 'CAL_TIME', VAR_NAME, COUNT_VAR_NAME]
                    # e.g., ['DEV_ID', 'CAL_TIME', 'COUNT_PM25', 'PM25']
                    if agg_var_df.empty:
                        continue
                    if is_for_minute==False:
                        hour_var_df = self.agg_agent.agg_minutes_to_hours(agg_var_df, var_name)
                    else:
                        print("不用做小时级别的聚合！")
                        #虚拟聚合（只有几条数据）
                        hour_var_df = self.agg_agent.agg_minutes_to_hours(agg_var_df, var_name, hour)
                    #合并各个污染物
                    full_df = self.agg_agent.merge_df(hour_var_df,full_df)
                org_vargroupid = self.agg_agent.concat_df(full_df,org_vargroupid)
            org[cap_x]=org_vargroupid
        if is_for_minute==False:
            # 回写数据库
            self.dao.write_org_db(org,hour)
            return org
        else:
            #不入数据库
            #返回数据放到内存里待用
            return org

    def agg_across_multi_sensors(self, var):
        pass

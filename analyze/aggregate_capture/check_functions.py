# encoding = utf-8

#导入自己的包

from log import log
from error_demo.error_code import *
logger = log.log_demo()

class CheckAgent():
    def __init__(self, config):
        """
        初始化类CheckAgent
        :param config: 单例配置项对象,去数据库里取对应的配置项
        """
        pollutants = ['PM25', 'PM10', 'SO2', 'CO', 'NO2', 'O3', 'TVOC', 'TSP', 'HUMIDITY', 'TEMPERATURE']
        self.abnormal_value_list = {}
        self.min_signal_val = {}
        self.max_signal_val = {}
        self.max_abs_delta = {}
        self.max_time_delta = {}

        for p in pollutants:
            self.abnormal_value_list[p] = config.get_config_var_data('abnormal_value_list', p)
            self.max_time_delta[p] = config.get_config_var_data('max_time_delta', p)
            self.max_abs_delta[p] = config.get_config_var_data('max_abs_delta', p)
            self.min_signal_val[p] = config.get_config_var_data('min_signal_val', p)
            self.max_signal_val[p] = config.get_config_var_data('max_signal_val', p)

        self.hop_periods = 1


    def clean_capture_data(self, df, var, channels):
        """
        对capture数据进行清理，包括的内容有
        2. 去掉已知异常编码数据并记录日志
        3. 去掉电化学传感器预热数据并记录日志
        4. 去掉不在原始信号测量范围内的数据
        5. 去掉跳变的数据
        :param df: 待处理的dataframe
        :param var: 污染物变量名（也是待处理的列名）
        :param channels: 通道列表list<string>
        :return: 经处理后的dataframe
        """
        # 由于capture数据可能重复，首先去重
        df = self.de_duplications(df)

        tmp_df = df[['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID']].copy()
        # logger.info('begin clean_capture_data')
        for channel in channels:
            cur_df = df[['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID', channel]].copy()
            #排除异常码
            cur_df = self.check_anormaly_code(cur_df, var, channel)
            if cur_df.empty:
                logger.debug('after check_anormaly_code there is no valid data!')
                continue
            else:
                #排除预热时间内的异常数据
                cur_df = self.check_warmup(cur_df, var, channel)
                if cur_df.empty:
                    logger.debug('after check_warmup there is no valid data!')
                    continue
                else:
                    # 排除不在范围的数据
                    cur_df = self.check_out_of_range(cur_df, var, channel)
                    if cur_df.empty:
                        logger.debug('after check_out_of_range there is no valid data!')
                        continue
                    else:
                        #排除跳变
                        cur_df = self.check_hop(cur_df, var, channel)
            tmp_df = tmp_df.merge(cur_df, how = 'left', on = ['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID'])
        # logger.info('***** clear end!')
        return tmp_df, None, None

    def de_duplications(self, df):
        """
        对过来的capture数据去重，同一时间上传的几条数据取第一条
        :param df:待处理的capture数据
        :return:去重后的df
        """
        #先按降序排序然后取第一条

        df.sort_values(by=['DEV_ID', 'CAP_TIME', 'MEA_POINT_ID'], axis=0, ascending=False, inplace=True)
        df = df.groupby(['DEV_ID', 'CAP_TIME']).first()
        df.reset_index(inplace=True)

        return df

    def check_anormaly_code(self, df, var, channel):
        """
        假设df是具体的某个capture_x pollutant vargroup 的数据
        2. 去掉已知异常编码数据并记录日志
        :param df: 待处理的capture数据
        :param var: 某个污染物 (用来取某污染物配置项的具体值)
        :param channel: 某个通道 （只是在多传感器的情况下进行通道区分）
        :return: 去除异常码后的df数据
        """
        dev_list1 = df['DEV_ID'].unique()
        # logger.info('before check_anormaly_code {} and devices have {}'.format(len(df), len(dev_list1)))

        abnormal_value_list = self.abnormal_value_list[var]
        df=df[~df[channel].isin(abnormal_value_list)]

        dev_list2 = df['DEV_ID'].unique()
        dev_list = set(dev_list1) - set(dev_list2)
        # logger.info('after check_anormaly_code {} and devices have {}'.format(len(df), len(dev_list2)))
        logger.debug('under var {} ,after check_anormaly_code devics diff:{}'.format(var, dev_list))
        return df


    def check_warmup(self, df, var, channel):
        """
        3. 去掉电化学传感器预热数据并记录日志
        :param df:待处理的df数据
        :param var:某个污染物 (用来取某污染物配置项的具体值)
        :param channel:某个通道 （只是在多传感器的情况下进行通道区分）
        :return:返回处理后的df数据
        """
        return df


    def check_hop(self, df, var, channel):
        """
        5. 去掉跳变的数据 去掉在一定时间范围内突变的那条数据，如果超出时间段就不算是跳变看做是随时间推移的正常变化
        :param df:待处理的df数据
        :param var:某个污染物 (用来取某污染物配置项的具体值)
        :param channel:某个通道 （只是在多传感器的情况下进行通道区分）
        :return:返回处理后的df数据
        """
        dev_list1 = df['DEV_ID'].unique()
        # logger.info('before check_hop {} and devices have {}'.format(len(df), len(dev_list1)))

        max_time_delta = self.max_time_delta[var]
        max_abs_delta = self.max_abs_delta[var]
        df.sort_values(['DEV_ID', 'CAP_TIME'], ascending=[False, True], inplace=True)
        df.reset_index(drop=True,inplace=True)
        df['PREV_'+channel]= df[channel].diff(periods=self.hop_periods)
        df.loc[0,'PREV_'+channel] = 0
        df['PREV_CAP_TIME']=df['CAP_TIME'].diff(periods=self.hop_periods)
        df.loc[0, 'PREV_CAP_TIME_SECOND'] = 0
        #将每个设备第一条差值设置为 0
        df['PREV_'+ channel] = df.apply(lambda x:x['PREV_'+ channel] if x.PREV_CAP_TIME.total_seconds()>=0 else 0,axis=1)
        df['PREV_CAP_TIME_SECOND'] = df.apply(lambda x:x.PREV_CAP_TIME.total_seconds() if x.PREV_CAP_TIME.total_seconds()>=0 else 0,axis=1)

        #判断是否在跳变场景下
        df[channel]=df.apply(lambda x:-99 if x['PREV_CAP_TIME_SECOND']<= max_time_delta and x['PREV_'+channel] >= max_abs_delta else x[channel],axis=1)
        df = df[~df[channel].isin([-99])]
        df.drop(['PREV_'+channel,'PREV_CAP_TIME','PREV_CAP_TIME_SECOND'],axis=1,inplace=True)

        dev_list2 = df['DEV_ID'].unique()
        dev_list = set(dev_list1) - set(dev_list2)
        # logger.info('after check_hop {} and devices have {}'.format(len(df), len(dev_list2)))
        logger.debug('under var {} ,after check_hop devics diff:{}'.format(var, dev_list))
        return df

    def check_out_of_range(self, df, var, channel):
        """
        4. 去掉不在原始信号测量范围内的数据
        :param df: 待处理的df数据
        :param var: 某个污染物 (用来取某污染物配置项的具体值)
        :param channel: 某个通道 （只是在多传感器的情况下进行通道区分）
        :return: 返回处理后的数据
        """
        dev_list1 = df['DEV_ID'].unique()
        # logger.info('before check_hop {} and devices have {}'.format(len(df), len(dev_list1)))

        min_val = self.min_signal_val[var]
        max_val = self.max_signal_val[var]
        df = df[(df[channel]<=max_val) & (df[channel]>=min_val)]

        dev_list2 = df['DEV_ID'].unique()
        dev_list = set(dev_list1) - set(dev_list2)
        # logger.info('after check_hop {} and devices have {}'.format(len(df), len(dev_list2)))
        logger.debug('under var {} ,after check_hop devics diff:{}'.format(var, dev_list))
        return df


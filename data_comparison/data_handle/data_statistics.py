import sys
import os
curpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.split(curpath)[0]
sys.path.append(rootpath)
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.header import Header
from utility import  mysql_connector
import pandas as pd
import datetime
import configparser


class DataStatistics():
    def __init__(self,target_time,permit_lyst):
        self.permit_dev_df = self.permit_sql_handle(permit_lyst)
        self.standard_data_df = self.sql_handle_cityid(target_time)
        self.standard_tvoc_df = self.sql_handle_cityid_tvoc(target_time)
        self.dev_by_city_df = self.city_dev_sql(target_time)
        self.city_list = self.get_city_list()
        self.city_name_df = self.get_city_name()
        self.var_name_df = self.var_name_sql()

    def sql_handle_cityid(self,target_time):
        """
        获取标准数据的df
        :param target_time: 目标时间
        :return: 标准数据的df
        """
        if self.permit_dev_df.empty:
            pid = ''
        else:
            self.permit_dev_lyst = self.permit_dev_df['SENSOR_ID'].tolist()
            pid = str(self.permit_dev_lyst)[1:-1]
        sql = "select sen.CITYID,abbr.VAR_TYPE_ID, count(sen.SENSOR_ID) from(select s.SENSOR_ID,CITYID,STATE from  SENSOR_INFO s,MEASURE_POINT m where s.MEASURE_POINT_ID=m.ID and CITYID>0 and STATE like '1%' and MEASURE_POINT_ID>0  and RELATE_SITE_ID<0 and SENSOR_ID not in (select SENSOR_ID from T_ABNORM_DEVICE where TIMESTAMP ='{}' ) and SENSOR_ID not in ({})) sen inner join  (select SENSOR_ID,VAR_TYPE_ID from T_SENSOR_TRANSDUCER_ABBR a join T_SENSOR_VARGROUP_MAP b on a.ABBR_CODE=b.ABBR_CODE) abbr on sen.SENSOR_ID=abbr.SENSOR_ID  group by sen.CITYID,abbr.VAR_TYPE_ID;".format(target_time,pid)
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        sql_data.dropna(axis=0,how='any',inplace=True)
        return sql_data

    def sql_handle_cityid_tvoc(self,target_time):
        """
        获取标准数据的df
        :param target_time: 目标时间
        :return: 标准数据的df
        """
        if self.permit_dev_df.empty:
            pid = ''
        else:
            self.permit_dev_lyst = self.permit_dev_df['SENSOR_ID'].tolist()
            pid = str(self.permit_dev_lyst)[1:-1]
        sql = "select sen.CITYID,abbr.VAR_TYPE_ID, count(sen.SENSOR_ID) from(select s.SENSOR_ID,CITYID,STATE from  SENSOR_INFO s,MEASURE_POINT m where s.MEASURE_POINT_ID=m.ID and CITYID>0 and STATE = '1100' and MEASURE_POINT_ID>0  and RELATE_SITE_ID<0 and SENSOR_ID not in (select SENSOR_ID from T_ABNORM_DEVICE where TIMESTAMP ='{}' ) and SENSOR_ID not in ({})) sen inner join  (select SENSOR_ID,VAR_TYPE_ID from T_SENSOR_TRANSDUCER_ABBR a join T_SENSOR_VARGROUP_MAP b on a.ABBR_CODE=b.ABBR_CODE) abbr on sen.SENSOR_ID=abbr.SENSOR_ID  group by sen.CITYID,abbr.VAR_TYPE_ID;".format(target_time,pid)
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        sql_data.dropna(axis=0, how='any', inplace=True)
        return sql_data

    def permit_sql_handle(self,permit_lyst):
        p_id_lyst = str(permit_lyst)[1:-1]
        sql = "select b.SENSOR_ID,a.P_ID,a.PNAME from PRIVILEGE_INFO a , PRIVILEGE_SENSOR_MAP b where a.P_ID in ({}) and a.P_ID=b.PRIVILEGE_ID;".format(p_id_lyst)
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        return sql_data

    def city_dev_sql(self,target_time):
        """
        获取城市和设备的对应列表
        :param target_time: 目标时间
        :return: 城市和设备的对应列表
        """
        if self.permit_dev_df.empty:
            pid = ''
        else:
            self.permit_dev_lyst = self.permit_dev_df['SENSOR_ID'].tolist()
            pid = str(self.permit_dev_lyst)[1:-1]
        sql = "select s.SENSOR_ID,CITYID from  SENSOR_INFO s,MEASURE_POINT m where s.MEASURE_POINT_ID=m.ID and CITYID>0 and STATE like '1%' and MEASURE_POINT_ID>0 and RELATE_SITE_ID<0 and SENSOR_ID not in (select SENSOR_ID from T_ABNORM_DEVICE where TIMESTAMP ='{}') and SENSOR_ID not in ({})".format(target_time,pid)
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        return sql_data

    def var_name_sql(self):
        """
        获取var_id和污染物的映射
        :return: var_id和污染物的映射
        """
        sql = "SELECT AQ_TYPE,NAME FROM T_DICT_AQ_TYPE"
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        return sql_data

    def city_name_sql_handle(self):
        """
        获取城市名字和城市id的映射
        :return: 城市名字和城市id的映射
        """
        sql = "SELECT PARENTNAME,CITYID FROM DIC_ZONE_ALL WHERE CITYID>0 AND DICLEVEL = 1"
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        return sql_data

    def get_dev_list_by_city(self,city_id):
        """
        通过城市获取对应的设备
        :param city_id:城市id
        :return:对应的设备列表
        """
        dev_list = self.dev_by_city_df[self.dev_by_city_df['CITYID'] == city_id]['SENSOR_ID'].tolist()
        return dev_list

    def adj_data_handle(self,table,target_time,var,dev_lyst):
        """
        查询当小时当污染物的数量
        :param table:数据库表名称
        :param target_time:目标时间
        :param var:污染物名称
        :param dev_lyst:设备范围
        :return:当小时当污染物的数量
        """
        sql = "SELECT COUNT(VAR_TYPE_ID) FROM {} WHERE ADJ_TIME = '{}' and VAR_TYPE_ID= '{}' AND DEV_ID IN ({})".format(table, target_time,var,dev_lyst)
        sql_data = mysql_connector.mysql_export_data_to_df(sql, 'MYSQL-SENSOR1')
        return sql_data

    def get_city_list(self):
        """
        获取城市列表
        :return: 城市列表
        """
        city_list = self.dev_by_city_df['CITYID'].unique().tolist()
        return city_list

    def get_city_name(self):
        """
        获取城市id与城市名字的映射
        :return:城市id与城市名字的映射
        """
        city_df = self.city_name_sql_handle()
        city_df.drop_duplicates(inplace=True)
        city_df = city_df.sort_values(by='CITYID')
        city_df = city_df.reset_index(drop=True)
        return city_df

    def data_handle(self,table,target_time,receivers,per_standard,threshold_one,threshold_two,dev_difference_one,dev_difference_two):
        """
        主函数，实现实际出数与标准出数的对比，如果缺数发送邮件
        :param table: 数据表名称
        :param target_time: 目标时间
        :param receivers: 接收邮件的邮箱
        :return: 无
        """
        city_lyst = []
        city_error_lyst = []
        vars_lyst = []
        var_error_lyst = []
        standard_num_lyst = []
        standard_num_error_lyst = []
        adj_num_lyst = []
        adj_num_error_lyst = []
        per_lyst = []
        per_error_lyst = []
        for city in self.city_list:
            city_name = self.city_name_df[self.city_name_df['CITYID'] == city]['PARENTNAME'].values[0]
            var_lyst = self.standard_tvoc_df[self.standard_tvoc_df['CITYID'] == city]['VAR_TYPE_ID'].tolist()
            dev_lyst = str(self.get_dev_list_by_city(city))[1:-1]
            for var in var_lyst:
                var_name = self.var_name_df[self.var_name_df['AQ_TYPE'] == var]['NAME'].values[0]
                city_lyst.append(city_name)
                vars_lyst.append(var_name)
                if var == 8:
                    df = self.standard_tvoc_df.copy()
                    df.set_index(['CITYID', 'VAR_TYPE_ID'], inplace=True)
                    standard_num = df.loc[(city, var), 'count(sen.SENSOR_ID)']
                    standard_num_lyst.append(standard_num)
                else:
                    standard_num = self.standard_data_df[(self.standard_data_df['CITYID'] == city) & (self.standard_data_df['VAR_TYPE_ID'] == var)]['count(sen.SENSOR_ID)'].values[0]
                    standard_num_lyst.append(standard_num)
                adj_df = self.adj_data_handle(table,target_time,var,dev_lyst)
                if adj_df.empty:
                    city_error_lyst.append(city_name)
                    var_error_lyst.append(var_name)
                    adj_num_lyst.append(0)
                    per_lyst.append(0)
                    standard_num_error_lyst.append(standard_num)
                    adj_num_error_lyst.append(0)
                    per_error_lyst.append(0)
                else:
                    adj_num = adj_df['COUNT(VAR_TYPE_ID)'].values[0]
                    adj_num_lyst.append(adj_num)
                    per_num = adj_num/standard_num
                    per_lyst.append(float('%.2f'%(per_num*100)))
                    if per_num*100 < per_standard:
                        if standard_num<threshold_one:
                            if standard_num-adj_num>dev_difference_one:
                                city_error_lyst.append(city_name)
                                var_error_lyst.append(var_name)
                                standard_num_error_lyst.append(standard_num)
                                adj_num_error_lyst.append(adj_num)
                                per_error_lyst.append(float('%.2f'%(per_num*100)))
                        elif standard_num<threshold_two:
                            if standard_num-adj_num>dev_difference_two:
                                city_error_lyst.append(city_name)
                                var_error_lyst.append(var_name)
                                standard_num_error_lyst.append(standard_num)
                                adj_num_error_lyst.append(adj_num)
                                per_error_lyst.append(float('%.2f'%(per_num*100)))
                        else:
                            city_error_lyst.append(city_name)
                            var_error_lyst.append(var_name)
                            standard_num_error_lyst.append(standard_num)
                            adj_num_error_lyst.append(adj_num)
                            per_error_lyst.append(float('%.2f' % (per_num * 100)))
        standard_dict = {'城市':city_lyst,'污染物':vars_lyst,'标准出数':standard_num_lyst,'实际出数':adj_num_lyst,'出数百分比(%)':per_lyst}
        error_dict = {'城市':city_error_lyst,'污染物':var_error_lyst,'标准出数':standard_num_error_lyst,'实际出数':adj_num_error_lyst,'出数百分比(%)':per_error_lyst}
        std_df = pd.DataFrame(standard_dict)
        std_df = std_df.sort_values(by='出数百分比(%)')
        std_df = std_df.reset_index(drop=True)
        error_df = pd.DataFrame(error_dict)
        error_df = error_df.sort_values(by='出数百分比(%)')
        error_df = error_df.reset_index(drop=True)
        error_df.to_csv('error.csv')
        std_df.to_csv('data.csv')
        if not error_df.empty:
            sentence = self.get_sentence(error_df, target_time)
            self.send_email(sentence,receivers)
        else:
            sentence = '很好哦，没问题呀!（>_<)!'
            self.send_email(sentence, receivers)

    def get_sentence(self,std_df,target_time):
        """
        编辑邮件内容主题
        :param std_df: 有异常信息的数据
        :param target_time: 目标时间
        :return: 邮件内容主题
        """
        sentence = ''
        for i in range(len(std_df)):
            city_name = std_df.loc[i]['城市']
            var_name = std_df.loc[i]['污染物']
            standard_num = std_df.loc[i]['标准出数']
            adj_num = std_df.loc[i]['实际出数']
            per_num = std_df.loc[i]['出数百分比(%)']
            sentence += str(i+1)+'.'+city_name+'在'+target_time+'时污染物类型为'+var_name+'的设备出数:'+str(adj_num)+'标准出数应:'+str(standard_num)+'出数率:'+str(per_num)+'%'+'\n'
        return sentence

    def send_email(self,sentence,receiver):
        """
        发送邮件函数
        :param sentence:邮件主题
        :param receiver:接收邮件的邮箱
        :return:无
        """
        smtp_server = 'smtp.163.com'
        user = 'xzj_123321@163.com'
        password = 'xzj19911010xzj'

        sender = 'xzj_123321@163.com'
        receivers = receiver

        for receiver in receivers:
            smtp = smtplib.SMTP_SSL(smtp_server, 465)
            smtp.helo()
            smtp.ehlo()
            smtp.login(user, password)
            message = MIMEMultipart()
            message['From'] = 'xzj_123321@163.com'
            message['To'] = Header('<' + receiver + '>', 'utf-8')
            subject = '当小时设备出数率汇报'
            message['Subject'] = Header(subject, 'utf-8')  # 标题
            message.attach(MIMEText(sentence, 'plain', 'utf-8'))  # content

            with open('data.csv', 'rb') as f:
                attach1 = MIMEText(f.read(), 'base64', 'utf-8')
                attach1["Content-Type"] = 'application/octet-stream'
                attach1["Content-Disposition"] = 'attachment; filename="Dev_Efficency.csv"'
                message.attach(attach1)

            with open('error.csv', 'rb') as f:
                attach1 = MIMEText(f.read(), 'base64', 'utf-8')
                attach1["Content-Type"] = 'application/octet-stream'
                attach1["Content-Disposition"] = 'attachment; filename="Dev_ERROR_Efficency.csv"'
                message.attach(attach1)

            print("message done!")
            try:
                smtp.sendmail(sender, receiver, message.as_string())
                smtp.quit()
                print(receiver + "发送成功 --> :)")
            except smtplib.SMTPException as e:
                print(e)
                print("Error, 无法发送邮件 --> >_<")


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config_permit.ini', encoding='utf-8')
    # permit_lyst = ['5449da247d674f1191273a39d64b0eb0 ','3f689ecd68f844ab96243f7e2569ae01']
    permit_lyst = config.get('permit', 'permit_lyst')[1:-1].split(',')
    # per_standard = 100
    per_standard = int(config.get('permit', 'per_standard'))
    print(per_standard)
    # receivers = ['649516524@qq.com', 'xuzijie@i2value.com']
    receivers = config.get('permit', 'receivers')[1:-1].split(',')
    print(receivers)
    check_time = datetime.datetime.now()+datetime.timedelta(hours=-1)
    table_date = check_time.strftime("%Y%m")
    print(table_date)
    target_time = check_time.strftime('%Y-%m-%d %H:00:00')
    print(target_time)
    table = 'DEVICE_ADJUST_VALUE_BYHOUR_'+table_date
    threshold_one = int(config.get('permit', 'dev_threshold_level_one'))
    print(threshold_one)
    threshold_two = int(config.get('permit', 'dev_threshold_level_two'))
    print(threshold_two)
    dev_difference_one = int(config.get('permit', 'dev_difference_level_one'))
    print(dev_difference_one)
    dev_difference_two = int(config.get('permit', 'dev_difference_level_two'))
    print(dev_difference_two)
    ds = DataStatistics(target_time,permit_lyst)
    ds.data_handle(table,target_time,receivers,per_standard,threshold_one,threshold_two,dev_difference_one,dev_difference_two)


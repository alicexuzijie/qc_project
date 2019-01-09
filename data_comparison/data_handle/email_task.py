import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.header import Header

def Send_Email(sentence):
    smtp_server = 'smtp.163.com'
    user = 'xzj_123321@163.com'
    password = 'xzj19911010xzj'

    # smtp = smtplib.SMTP_SSL(smtp_server, 465)
    # smtp.helo()
    # smtp.ehlo()
    # smtp.login(user, password)

    sender = 'xzj_123321@163.com'
    receivers = ['649516524@qq.com', 'xuzijie@i2value.com']

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

        print("message done!")
        try:
            smtp.sendmail(sender, receiver, message.as_string())
            smtp.quit()
            print(receiver + "发送成功 --> :)")
        except smtplib.SMTPException as e:
            print(e)
            print("Error, 无法发送邮件 --> >_<")

if __name__ == '__main__':
    sentence = '测试一下'
    Send_Email(sentence)
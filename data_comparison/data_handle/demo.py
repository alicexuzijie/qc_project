import datetime

target_time = datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')
print(target_time)
res = datetime.datetime.now().strftime('%Y%m')
print(res)
# str = target_time.split('-')
# res = str[0]+str[1]
# print(str)
# print(res)
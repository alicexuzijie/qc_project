import schedule
import time


def job():
    print('do something')


schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
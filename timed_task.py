import datetime
import time


def timerfun(sched_Timer):
    flag = 0
    while True:
        now=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        time.sleep(1)
        print(now)
        if now==sched_Timer:
            print('do something')
            flag = 1
        else:
            if flag==1:
                sched_Timer = datetime.datetime.strptime(sched_Timer, '%Y-%m-%d %H:%M:%S')
                sched_Timer = sched_Timer+datetime.timedelta(minutes=1)
                flag = 0


if __name__ == '__main__':
    sched_Timer=datetime.datetime(2018,10,24,11,40,0).strftime('%Y-%m-%d %H:%M:%S')
    print('run the timer_task at {}'.format(sched_Timer))
    timerfun(sched_Timer)

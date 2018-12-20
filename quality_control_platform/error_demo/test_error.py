import common
from error_code import *
from log import log
import json

logger = log.log_demo()


class Test:
    def demo_custom(self, i):
        # 当i=3,4时会抛出自定义异常
        if i > 2:
            raise RuntimeException("数据太大", '1100',"i>2")

    def demo_system(self, i):
        # 直接抛出系统异常
        a = [0]
        i += 1
        print(a[i])

    def demo_outer(self, i):
        a = [0,1]
        i += 1

        if i == 1:
            raise RuntimeException("需要警告", '1101', "a[%d]" % i)  #它会抛给最近except去捕获
        try:
            print(a[i])
        except Exception as x:
            raise RuntimeException("内层异常捕捉", '1001', "a[%d]"%i)

    def demo_inner(self, i):
        a = [0,1]
        i += 1
        try:
            if i > 1:
                raise RuntimeException("警告", '1101', "a[%d]" % i)  #它会抛给最近except去捕获
            print(a[i])
        except Exception as x:
            raise RuntimeException("内层异常捕捉", '1001', "a[%d]"%i)

    def test(self):

        mResultList = []
        for i in range(5):
            try:
                # self.demo_custom(i)
                # self.demo_system(i)
                # self.demo_outer(i)
                self.demo_inner(i)

            except RuntimeException as x:
                mResult = Result()
                mResult.name = x.name
                mResult.code = x.code
                mResult.description = x.description
                mResult.setData({})
                mResultList.append(mResult)
                if x.code[1] == '1':
                    print("自定义异常")
                    continue
                else:
                    return mResultList

            except Exception as e:
                mResult = Result()
                mResult.name = "Error"
                mResult.code = "1001"
                mResult.description = str(e)
                mResult.setData({})
                mResultList.append(mResult)
                logger.exception(e, exc_info=False)
                return mResultList

        mResult = Result()
        mResult.name = "Ok"
        mResult.code = "1000"
        mResult.description = "运行正确"
        mResult.setData([1, 3, 5], {2: 3}, 4, 5, i)
        mResultList.append(mResult)
        msg = json.dumps(mResult.getData())
        logger.error('code:%s,name:%s,description:%s,data:%s',
                     mResult.code, mResult.name, mResult.description, mResult.getData())
        # logger.error('It is a error,data:%s', msg,
        #              extra={'code':mResult.code, 'errorname':mResult.name, 'description':mResult.description})
        return mResultList


if __name__=="__main__":
    mTest = Test()
    error_lyst = mTest.test()
    for error_id in error_lyst:
        print(error_id.code)
        if error_id.code[1] ==  '1':
            print("警告")
        elif error_id.code == "1001":
            print("错误")
        else:
            print("正确")
        # print(error_id.name)
        # print(error_id.description)
        # print(error_id.getData())

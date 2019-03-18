class BaseError(Exception):
    """
    基础异常类，创建基础异常类储存异常名称、异常信息、异常错误码以及异常额外信息。
    """
    def __init__(self, name, message, code):
        """
        初始化基础异常类，将异常名称、异常信息、异常错误码储存在对象内
        :param name: 异常名称
        :param message: 异常信息
        :param code: 异常错误码
        """
        self.name = name
        self.message = message
        self.code = code

    def setdata(self, *args):
        """
        储存信息函数，用于存储对错误对象的额外描述信息。
        :param args: 需储存的额外信息。
        :return: 将需储存的额外信息作为属性储存
        """
        self.data=args

    def getdata(self):
        """
        获取储存的额外信息。
        :return: 获取储存的额外信息
        """
        return self.data


class EmptyDfError(BaseError):

    def __init__(self, message):
        super(EmptyDfError, self).__init__('EmptyDfError', message, 1101)


class NullFieldError(BaseError):

    def __init__(self, message):
        super(NullFieldError, self).__init__('NullFieldError', message, 1102)

class NoneDfError(BaseError):

    def __init__(self, message):
        super(NoneDfError, self).__init__('NoneDfError', message, 1103)


class SqlValueError(BaseError):

    def __init__(self, message):
        super(SqlValueError, self).__init__('SqlValueError', message, 1203)


class ParameterRangeError(BaseError):

    def __init__(self, message):
        super(ParameterRangeError, self).__init__('ParameterRangeError', message, 2101)


class InnerParameterError(BaseError):

    def __init__(self, message):
        super(InnerParameterError, self).__init__('InnerParameterError', message, 2102)


class ArrayNotMatchError(BaseError):
    def __init__(self, message):
        super(ArrayNotMatchError, self).__init__('ArrayNotMatchError', message, 2103)


class ZeroLengthError(BaseError):
    def __init__(self, message):
        super(ZeroLengthError, self).__init__('ZeroLengthError', message, 2104)


class ValueRangeError(BaseError):

    def __init__(self, message):
        super(ValueRangeError, self).__init__('ValueRangeError', message, 2201)


class CaptureValueError(BaseError):

    def __init__(self, message):
        super(CaptureValueError, self).__init__('CaptureValueError', message, 3101)


class OrgValueError(BaseError):

    def __init__(self, message):
        super(OrgValueError, self).__init__('OrgValueError', message, 3201)


class SiteValueError(BaseError):

    def __init__(self, message):
        super(SiteValueError, self).__init__('SiteValueError', message, 3301)



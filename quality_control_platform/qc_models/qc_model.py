# coding = utf-8
from abc import abstractmethod, ABCMeta

class QualityControlModel(metaclass=ABCMeta):
    """
    质控模型的基类，所有其他模型应该继承这个类，并且实现相应的接口
    """

    @abstractmethod
    def __init__(self, model_name):
        """
        初始化函数
        @param: model_name 模型名称，比如linear_regression, svr等
        """
        self.model_name = model_name
        self.is_valid = False

    @abstractmethod
    def decide_validity(self, var):
        """
        模型是否可信
        return: 模型是否可信的布尔值
        """
        pass

    def is_valid(self):
        return self.is_valid

    @abstractmethod
    def train(self, features, y):
        """
        给定训练数据在这里得到模型并且赋值给self.model
        """
        pass

    @abstractmethod
    def predict(self, features):
        """
        给定多行features，每一行对应于一个需要预测的点
        @param: features，dataframe，每一行为不同的非质控点在当小时的特征变量，每一列为不同的特征值

        return: 预测值，单列dataframe
        """
        pass

    @abstractmethod
    def save_model(self, dir, model_file_name):
        """
        把模型存储到相应的位置，可以考虑多一个重载函数是写库
        1. 写库应该需要序列化，先要看看看有没有相应的包支持序列化和反序列化
        2. 需要考虑模型的大小对网络传输的影响
        """
        pass

    @abstractmethod
    def restore_model(self, dir, model_file_name):
        """
        从文件或者数据库中恢复模型，用于为其他非质控点进行传递
        """
        pass

    @abstractmethod
    def set_humidity_mode(self, var, hum=-1):
        pass

    @abstractmethod
    def get_humidity_mode(self):
        pass


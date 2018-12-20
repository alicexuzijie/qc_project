import logging.config
import logging.handlers
import os
import sys
import common


def log_demo():
    """
    日志类，通过读取配置文件的相关配置信息生成执行文件名的logger
    :return: 执行文件名的日志对象
    """
    # print(os.path.abspath(sys.argv[0]).split('\\')[-2])
    config_file = '%s/logging_conf.ini' % os.path.dirname(__file__)
    logging.config.fileConfig(config_file, disable_existing_loggers=False)
    file = os.path.abspath(sys.argv[0]).replace('\\','/').split('/')[-2]
    logger = logging.getLogger(str(file))
    return logger


def main():
    pass


if __name__ == '__main__':
    main()









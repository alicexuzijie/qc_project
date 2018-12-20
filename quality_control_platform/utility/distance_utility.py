# encoding = utf-8
import numpy as np

import common
from error_demo.error_code import *

def weighted_mean_by_distance(power_index, distance_list, value_list):
    '''
    给定一系列的距离和值，按照距离的power次方取加权
    '''
    if len(distance_list) == 0 or len(value_list) == 0:
        # $$$ 这里要raise error
        raise ZeroLengthError('distance list或value list长度为0')
    elif len(distance_list) != len(value_list):
        # $$$ 这里要raise error
        # print('The number of distances does not equal to the number of values!')
        raise ArrayNotMatchError('distance list和value list的长度不一致')
    else:
        distance_with_power = np.power(np.divide(1, distance_list), power_index)

        return np.dot(distance_with_power, value_list)/np.sum(distance_with_power)

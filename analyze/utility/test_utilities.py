
import unittest
import time_utility as tu
import distance_utility as du

class UtilityTester(unittest.TestCase):
    def test_string_to_int_hour(self):
        my_str = '2018-09-01 12:12:20'
        int_hour_str = '2018-09-01 12:00:00'

        my_date_time = tu.str_datetime_to_int_hour(my_str)
        int_date_time = tu.time_str_to_datetime(int_hour_str)

        self.assertEqual(my_date_time, int_date_time)

def test_distance_utility():
    distance_list = [1, 2, 3]
    value_list = [2.5, 3.2, 0]
    print(du.weighted_mean_by_distance(2, distance_list, value_list))

# unittest.main()

test_distance_utility()

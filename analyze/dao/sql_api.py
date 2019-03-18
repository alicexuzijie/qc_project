# -*- coding: utf-8 -*-
from abc import abstractmethod, ABCMeta


class DataOperations(metaclass=ABCMeta):
    def __init__(self):
        self.field_names = {}
        self.field_name_full_set = ('PM25', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'TVOC', 'TSP', 'OUTSIDE_HUMIDITY', 'OUTSIDE_TEMPERATURE')

    @abstractmethod
    def query_active_devices(self):
        return

    @abstractmethod
    def query_active_devices_by_city(self, city_list):
        pass

    @abstractmethod
    def query_active_devices_by_device_list(self, device_list):
        pass

    @abstractmethod
    def query_channels(self):
        pass

    @abstractmethod
    def query_qualitycontrol_version(self):
        pass

    @abstractmethod
    def query_field_name(self):
        pass

    @abstractmethod
    def query_field_names_by_capture_x(self, device_info_df):
        pass

    @abstractmethod
    def query_capture_data_by_capture_x(self, hour, capture_x, field_name, device_list):
        pass

    @abstractmethod
    def query_capture_data_by_hour(self, hour, device_info_df):
        pass

    @abstractmethod
    def query_consistency_model(self):
        pass

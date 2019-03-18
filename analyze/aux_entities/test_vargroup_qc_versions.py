# encoding = utf-8

import sys

import common

from vargroup_qc_versions import VargroupQCVersions
from dao.mysql_impl import DataOperationsByMysql


def test_vargroup_qc_verrsions():
    dao = DataOperationsByMysql()
    vg_versions = VargroupQCVersions(dao)

    print(vg_versions.qc_versions_by_vargroup_and_var)


def test_get_qc_version():
    dao = DataOperationsByMysql()
    vg_versions = VargroupQCVersions(dao)
    vargroup_id = 'YSRDAQ0700'
    var_name = 'PM25'
    print(vg_versions.get_qc_version_by_vargroup_and_var(vargroup_id, var_name))


def test_verrsions_qc_vargroup():
    dao = DataOperationsByMysql()
    vg_versions = VargroupQCVersions(dao)
    print(vg_versions.qc_vargroup_by_versions_and_var)


def test_get_qc_group():
    dao = DataOperationsByMysql()
    vg_versions = VargroupQCVersions(dao)
    version = '2'
    var_name = 'PM25'
    print(vg_versions.get_qc_vargroup_by_version_and_var(version, var_name))


def test_get_qc_dev():
    dao = DataOperationsByMysql()
    vg_versions = VargroupQCVersions(dao)
    df_group = dao.query_active_devices()
    version = '1'
    var_name = 'TSP'
    print(vg_versions.get_qc_dev_by_version_and_var(version, var_name, df_group))


if __name__ == '__main__':
    test_vargroup_qc_verrsions()
    test_get_qc_version()
    test_verrsions_qc_vargroup()
    test_get_qc_group()
    test_get_qc_dev()
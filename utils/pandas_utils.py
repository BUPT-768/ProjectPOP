# coding=utf-8
import pandas as pd

__author__ = 'jayvee'
'''
Pandas的一些常用工具集
'''


def str2datetime(date_str, date_format='%Y%m%d'):
    """
    input str, output pandas' datetime obj
    :param date_str:
    :param date_format: default is '%Y%m%d'
    :return:
    """
    return pd.to_datetime(date_str, infer_datetime_format=True, format=date_format)

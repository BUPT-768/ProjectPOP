# coding=utf-8
import pandas as pd

__author__ = 'jayvee, jiaying.lu, zhouxiaojiang'
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


def normalize_max_min(df, col_name, replace=0):
    '''
    normalize specific column using max min normalization method
    最终效果是在df 中新加一列: col_name_normalized, 存放归一化后的值

    Args:
        df: pandas.dataFrame
        col_name: string
        replace: int, 0 代表新增一列存储，1 代表覆盖原始列, 默认不覆盖

    Returns:
        df: pandas.dataFrame
    '''
    mean_num = df[col_name].mean()
    min_num = df[col_name].min()
    max_num = mean_num + 2 * df[col_name].std()
    col_normalized = (df[col_name] - min_num) / (max_num - min_num) 
    if replace:
        col_name_normalized = col_name
    else:
        col_name_normalized = '%s_normalized' % (col_name)
    df[col_name_normalized] = col_normalized.map(lambda elem: min(elem, 1))
    return df


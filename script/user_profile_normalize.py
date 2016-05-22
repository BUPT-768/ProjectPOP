# -*- coding:utf-8 -*-

'''
对生成的user画像的数据进行归一化处理

author:zhouxiaojiang
CreateDate: 2016/05/21

'''

from __future__ import division
import os
import sys
import datetime
import pandas
import collections

abs_path = os.path.dirname(os.path.abspath(__file__))
abs_father_path = os.path.dirname(abs_path)
sys.path.append(abs_father_path)

from utils.log_tool import feature_logger as logger
from utils.basic_configs import TotalDays
from utils.basic_configs import timer

feature_dir = '%s/feature' % (abs_father_path)

@timer
def generate_obj_df(user_profile_path = '%s/user_analysis.csv' % feature_dir):
    '''
    load csv to a pandas.dataFrame
    param:file path
    return:
    '''
    pd = pandas.read_csv(user_profile_path)
    return pd

def normalize_num_type(pd,column):
    '''
    normalize date type data to [0,1]
    param:pandas.dataFrame,column name
    return:
    '''
    min_num = pd[column].min()
    mean_num = pd[column].mean()
    std_num = pd[column].std()
    max_num = mean_num + 2 * std_num
    pd[column] = (pd[column] - min_num) / (max_num - min_num) 
    return pd

def normalize_date_type(pd,column):
    '''
    normalize num type data to [0,1]
    param:pandas.dataFrame,column name
    return:
    '''
    pd[column] = pd[column] / TotalDays
    return pd

def normalize():
    df = generate_obj_df()
    for column in df.columns:
        if '天数' in column:
            logger.info('date type data')
            normalize_date_type(df,column)
        elif '数量' in column:
            logger.info('%s column' % column)
            normalize_num_type(df,column)
    ## 写归一化的数据
    df.to_csv('%s/feature/user_profile_normalize.csv' % abs_father_path)
if __name__ == '__main__':
    normalize()
    


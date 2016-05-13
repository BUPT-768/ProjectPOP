# -*- coding: utf-8 -*-
'''
Easy and basic configure for print log

Author: lujiaying
Date: 2016/05/12
'''

import logging
from logging.handlers import RotatingFileHandler
import os

# 用以控制是否输出到屏幕，线上环境不输出到屏幕
DebugConf = True
#DebugConf = False

abs_path = os.path.dirname(os.path.abspath(__file__))
abs_father_path = os.path.dirname(abs_path)
log_dir_path = abs_father_path + '/log'
if not os.path.exists(log_dir_path):
    os.makedirs(log_dir_path)

feature_logger = logging.getLogger('feature')
model_logger = logging.getLogger('model')

formatter = logging.Formatter('[%(asctime)s][pid:%(process)s] %(module)s.%(funcName)s: %(levelname)s: %(message)s')

# StreamHandler for print log to console
hdr = logging.StreamHandler()
hdr.setFormatter(formatter)
hdr.setLevel(logging.DEBUG)

# RotatingFileHandler
fhr_model = RotatingFileHandler('%s/model.log'%(log_dir_path), maxBytes=10*1024*1024, backupCount=3)
fhr_model.setFormatter(formatter)
fhr_model.setLevel(logging.DEBUG)

# RotatingFileHandler
fhr_feature = RotatingFileHandler('%s/feature.log'%(log_dir_path), maxBytes=10*1024*1024, backupCount=3)
fhr_feature.setFormatter(formatter)
fhr_feature.setLevel(logging.DEBUG)

model_logger.addHandler(fhr_model)
model_logger.addHandler(hdr)
model_logger.setLevel(logging.DEBUG)

feature_logger.addHandler(fhr_feature)
feature_logger.addHandler(hdr)
feature_logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    '''
    Usage:

    from utils.log_tool import model_logger as logger
    '''
    model_logger.debug('test from model_logger')
    feature_logger.debug('test from feature_logger')

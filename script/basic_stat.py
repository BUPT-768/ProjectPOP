# -*- coding: utf-8 -*-
'''
Statistics basic info

Author: lujiaying
Date: 2016/05/12
'''
import os
import sys
import pandas

abs_path = os.path.dirname(os.path.abspath(__file__))
abs_father_path = os.path.dirname(abs_path)
sys.append(abs_father_path)
from utils.log_tool import model_logger as logger

data_source_dir = '%s/data_source' % (abs_father_path)


def main():
    pass


# 艺人维度的统计
## 艺人平均歌曲发行量，平均歌曲发行周期, 平均初始播放数，语言从1~X, 性别


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
'''
Merge features for model input

Author: lujiaying
CreateDate: 2016/05/12
UpdateDate: 2016/05/19
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
from utils.file_utils import get_song_artist_map
from utils.basic_configs import ActionMap

data_source_dir = '%s/data_source' % (abs_father_path)
feature_dir = '%s/feature' % (abs_father_path)


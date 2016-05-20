# -*- coding: utf-8 -*-
'''
生成标签，key为<uid,artist_id,date>

Author: lujiaying
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
from utils.file_utils import get_song_artist_map, get_all_artist_id
from utils.basic_configs import ActionMap, TotalDays, PositiveLabel, NegativeLabel

data_source_dir = '%s/data_source' % (abs_father_path)
feature_dir = '%s/feature' % (abs_father_path)

def generate_label(source_file, output_file):
    '''
    生成标签文件

    Args:
        source_file: string
        output_file: string
    '''
    logger.info('Enter generate_label')
    user_dict = {}   # e.g. {uid:{date1:artist_id_set(), date2:artist_id_set()}}
    song_artist_map = get_song_artist_map()

    # step 1: read raw data
    with open(source_file) as fopen:
        fopen.readline()
        for line in fopen:
            user_id, song_id, gmt_create, action_type, Ds = line.strip().split(',')
            artist_id = song_artist_map[song_id]
            action = ActionMap[action_type]
            if action == 'play':
                if user_id not in user_dict:
                    user_dict[user_id] = {}
                if Ds not in user_dict[user_id]:
                    user_dict[user_id][Ds] = set()
                user_dict[user_id][Ds].add(artist_id)

    # step 2: generate label
    logger.info('Start step 2: generate label')
    user_cnt = 0
    output_result = []  # e.g. [[uid, aid, date, label], []]
    artist_id_set = get_all_artist_id()
    for user_id, info_dict in user_dict.iteritems():
        if user_cnt % 100000 == 0:
            logger.info('current stage: #%s user finished generate label' % (user_cnt))
        user_cnt += 1
        for date, positive_artist_id_set in info_dict.iteritems():
            for artist_id in positive_artist_id_set:
                output_result.append([user_id, artist_id, date, PositiveLabel])
            for artist_id in (artist_id_set - positive_artist_id_set):
                output_result.append([user_id, artist_id, date, NegativeLabel])

    # step 3: output result
    logger.info('Start output')
    with open(output_file, 'w') as fopen:
        fopen.write('user_id, artist_id, date, label\n')
        for cur_line in output_result:
            fopen.write(','.join(cur_line) + '\n')
            
    logger.info('End generate_label')

if __name__ == '__main__':
    source_file = '%s/mars_tianchi_user_actions.csv' % (data_source_dir)
    output_file = '%s/train_label.csv' % (feature_dir)
    generate_label(source_file, output_file)

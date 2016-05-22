# -*- coding: utf-8 -*-
'''
生成标签，key为<uid,artist_id,date>, 按日期分文件存储

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

def generate_label(source_file, output_dir):
    '''
    生成标签文件

    Args:
        source_file: string
        output_dir: string
    '''
    logger.info('Enter generate_label')
    date_dict = {}   # e.g. {date1:{uid1:artist_id_set(), uid2:artist_id_set()}, date2:{}}
    song_artist_map = get_song_artist_map()

    # step 1: read raw data
    with open(source_file) as fopen:
        fopen.readline()
        for line in fopen:
            user_id, song_id, gmt_create, action_type, Ds = line.strip().split(',')
            artist_id = song_artist_map[song_id]
            action = ActionMap[action_type]
            if action == 'play':
                if Ds not in date_dict:
                    date_dict[Ds] = {}
                if user_id not in date_dict[Ds]:
                    date_dict[Ds][user_id] = set()
                date_dict[Ds][user_id].add(artist_id)

    # step 2: generate label && output
    logger.info('Start step 2: generate label')
    artist_id_set = get_all_artist_id()
    for date, info_dict in date_dict.iteritems():
        logger.info('Current stage: %s user start generate labels' % (date))
        output_result = []  # e.g. [[uid, aid, date, label], []]
        for user_id, positive_artist_id_set in info_dict.iteritems():
            for artist_id in positive_artist_id_set:
                output_result.append([str(user_id), str(artist_id), str(date), str(PositiveLabel)])
            for artist_id in (artist_id_set - positive_artist_id_set):
                output_result.append([str(user_id), str(artist_id), str(date), str(NegativeLabel)])
        logger.info('Current stage: %s user start output to file' % (date))
        cur_dir = '%s/%s' % (output_dir, date)
        if not os.path.exists(cur_dir):
            os.makedirs(cur_dir)
        cur_file = '%s/train_label.csv' % (cur_dir)
        with open(cur_file, 'w') as fopen:
            fopen.write('user_id, artist_id, date, label\n')
            for cur_line in output_result:
                fopen.write(','.join(cur_line) + '\n')
        logger.info('Current stage: %s user output success, store in %s' % (date, cur_file))

    logger.info('End generate_label')
    return True


if __name__ == '__main__':
    source_file = '%s/mars_tianchi_user_actions.csv' % (data_source_dir)
    output_dir = '%s/dailyFeature' % (feature_dir)
    generate_label(source_file, output_dir)

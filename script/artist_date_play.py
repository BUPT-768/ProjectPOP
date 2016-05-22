# -*- coding: utf-8 -*-
'''
统计每个艺人的播放情况

Author: lujiaying
Date: 2016/05/17
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

data_source_dir = '%s/data_source' % (abs_father_path)


def main():
    # 艺人维度的统计
    ## step 1 读数据
    song_artist_map = {}
    logger.info('start read song artist map')
    with open('%s/data_source/mars_tianchi_songs.csv' % (abs_father_path)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            artist_id, publish_time, song_init_plays, language, gender = line_list[1], line_list[2], line_list[3], line_list[4], line_list[5]
            song_id = line_list[0]
            song_artist_map[song_id] = artist_id

    logger.info('start read table songs')
    artist_dict = {} # {'aid1': {'ds1':1, 'ds2':0}}
    with open('%s/data_source/mars_tianchi_user_actions.csv' % (abs_father_path)) as fopen:
        fopen.readline()
        for line in fopen:
            user_id,song_id,gmt_create,action_type,Ds = line.strip().split(',')
            artist_id = song_artist_map[song_id]
            if artist_id not in artist_dict:
                artist_dict[artist_id] = {}
            if Ds not in artist_dict[artist_id]:
                artist_dict[artist_id][Ds] = 0
            if action_type == '1':
                artist_dict[artist_id][Ds] += 1

    ## step 3 写统计
    logger.info('Start output')
    with open('%s/feature/artist_play_lu.csv' % (abs_father_path), 'w') as fopen:
        fopen.write('artist_id,date,play\n')
        for artist_id, info_dict in artist_dict.iteritems():
            for date, play in sorted(info_dict.iteritems(), key=lambda e:int(e[0])):
                fopen.write('%s,%s,%s\n' % (artist_id, date, play))
    

if __name__ == '__main__':
    main()

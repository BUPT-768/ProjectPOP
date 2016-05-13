# -*- coding: utf-8 -*-
'''
Statistics basic info

Author: lujiaying
Date: 2016/05/12
'''
from __future__ import division
import os
import sys
import pandas
import collections
abs_path = os.path.dirname(os.path.abspath(__file__))
abs_father_path = os.path.dirname(abs_path)
sys.append(abs_father_path)
from utils.log_tool import model_logger as logger

data_source_dir = '%s/data_source' % (abs_father_path)

def main():
    # 艺人维度的统计
    ## step 1 读数据
    logger.info('start read table songs')
    artist_dict = {}
    with open('%s/data_source/mars_tianchi_songs.csv') as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            artist_id, publish_time, song_init_plays, language, gender = line_list[1], line_list[2], line_list[3], line_list[4], line_list[5]
            if artist_id not in artist_dict:
                artist_dict[artist_id] = {'song_num':0, 'publish_time_list':[], 'song_init_plays_sum':0, 'l1':0, 'l2':0, 'l3':0, 'l4':0, 'l5':0, 'gender':gender}
            artist_dict[artist_id]['song_num'] += 1
            artist_dict[artist_id]['publish_time_list'].append(publish_time)
            artist_dict[artist_id]['song_init_plays_sum'] += song_init_plays
            if int(language) < 5:
                l_key = 'l%s' % (language)
            else:
                l_key = 'l5'
            artist_id[artist_id][l_key] += 1

    ## step 2 计算
    logger.info('Start cal features')
    for artist_id, info_dict in artist_dict.iteritems():
        info_dict['avg_song_init_plays'] = info_dict['song_init_plays_sum'] / info_dict['song_num']
        ### 统计平均歌曲发行周期，如果只有一首歌，置-1
        publist_time_list = [int(e) for e in info_dict['publist_time_list']
        start_day, end_day = max(publist_time_list), min(publist_time_list)
        if start_day == end_day:
            info_dict['avg_publish_cycle']

    ## step 3 写统计
    logger.info('Start output')
    with open('%s/feature/artist_stat_lu.csv', 'w') as fopen:
        fopen.write('艺人歌曲发行量, 平均歌曲发行周期(天), 平均初始播放数, 语言0, 语言1, 语言2, 语言3, 语言4, 语言>=5, 性别\n')
        pass
         
    

if __name__ == '__main__':
    main()


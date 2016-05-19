# -*- coding: utf-8 -*-
'''
Statistics basic info

Author: lujiaying
Date: 2016/05/12
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
    logger.info('start read table songs')
    artist_dict = {}
    with open('%s/data_source/mars_tianchi_songs.csv' % (abs_father_path)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            artist_id, publish_time, song_init_plays, language, gender = line_list[1], line_list[2], line_list[3], line_list[4], line_list[5]
            if artist_id not in artist_dict:
                artist_dict[artist_id] = {'song_num':0, 'publish_time_list':[], 'song_init_plays_sum':0, 'l0':0, 'l1':0, 'l2':0, 'l3':0, 'l4':0, 'l5':0, 'gender':gender}
            artist_dict[artist_id]['song_num'] += 1
            artist_dict[artist_id]['publish_time_list'].append(publish_time)
            artist_dict[artist_id]['song_init_plays_sum'] += int(song_init_plays)
            if int(language) < 5:
                l_key = 'l%s' % (language)
            else:
                l_key = 'l5'
            artist_dict[artist_id][l_key] += 1

    ## step 2 计算
    logger.info('Start cal features')
    for artist_id, info_dict in artist_dict.iteritems():
        ### 统计平均播放数
        info_dict['avg_song_init_plays'] = info_dict['song_init_plays_sum'] / info_dict['song_num']
        ### 统计平均歌曲发行周期，如果只有一首歌，置-1
        publist_time_list = [int(e) for e in info_dict['publish_time_list']]
        start_day, end_day = min(publist_time_list), max(publist_time_list)
        if start_day == end_day:
            info_dict['avg_publish_cycle'] = -1  # 代表无法统计
        else:
            start_day_datetime = datetime.datetime.strptime(str(start_day), '%Y%m%d')
            end_day_datetime = datetime.datetime.strptime(str(end_day), '%Y%m%d')
            info_dict['avg_publish_cycle'] = (end_day_datetime - start_day_datetime).days / info_dict['song_num']

    ## step 3 写统计
    logger.info('Start output')
    with open('%s/feature/artist_stat_lu.csv' % (abs_father_path), 'w') as fopen:
        fopen.write('艺人id,艺人歌曲发行量,平均歌曲发行周期(天),平均初始播放数,语言0,语言1,语言2,语言3,语言4,语言>=5,性别\n')
        for artist_id, info_dict in artist_dict.iteritems():
            fopen.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (artist_id, info_dict['song_num'], info_dict['avg_publish_cycle'],
                        info_dict['avg_song_init_plays'], info_dict['l0'], info_dict['l1'], info_dict['l2'], info_dict['l3'], info_dict['l4'],
                        info_dict['l5'], info_dict['gender']))
    

if __name__ == '__main__':
    main()

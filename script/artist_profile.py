# -*- coding: utf-8 -*-
'''
Statistics basic info

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

def process_artist_songs(artist_dict={}):
    '''
    处理songs文件
    Args:
        artist_dict: dict, key:artist_id, value:{}

    Returns:
        artist_dict: dict, e.g {artist_id1:{'song_num':0, 'publish_time_list':[], 'song_init_plays_sum':0, 'l0-5':0, 'gender':0}, artist_id2:{}}
    '''
    ## step 1 读数据
    logger.info('start read table songs')
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
    return artist_dict


def process_user_actions(artist_dict):
    '''
    Args:
        artist_dict: dict, key:artist_id, value:{}

    Returns:
        artist_dict: dict, key:artist_id, value:{}
    '''
    logger.info('Start process_user_actions')
    song_artist_map = get_song_artist_map()
    # step 1: read from raw data
    with open('%s/data_source/mars_tianchi_user_actions.csv' % (abs_father_path)) as fopen:
        fopen.readline()
        for line in fopen:
            user_id, song_id, gmt_create, action_type, Ds = line.strip().split(',')
            artist_id = song_artist_map[song_id]
            action = ActionMap[action_type]
            if not 'dates' in artist_dict[artist_id]:
                artist_dict[artist_id]['dates'] = {}
            if not Ds in artist_dict[artist_id]['dates']:
                artist_dict[artist_id]['dates'][Ds] = {'play_pv':0, 'download_pv':0, 'collect_pv':0, 'play_uids':set(), 'download_uids':set(), 'collect_uids':set(), 'play_songs':set(), 'download_songs':set(), 'collect_songs':set()}
            artist_dict[artist_id]['dates'][Ds][action+'_pv'] += 1
            artist_dict[artist_id]['dates'][Ds][action+'_uids'].add(user_id)
            artist_dict[artist_id]['dates'][Ds][action+'_songs'].add(song_id)

    logger.info('Start step 2 in process_user_actions')
    # step 2: calculate
    for artist_id, info_dict in artist_dict.iteritems():
        dates_dict = info_dict['dates']
        total_days = len(dates_dict)
        play_pv, download_pv, collect_pv = 0, 0, 0
        play_uv, download_uv, collect_uv = 0, 0, 0
        play_song_cnt, download_song_cnt, collect_song_cnt = 0, 0, 0
        for date_dict in dates_dict.itervalues():
            play_pv += date_dict['play_pv']
            download_pv += date_dict['download_pv']
            collect_pv += date_dict['collect_pv']
            play_uv += len(date_dict['play_uids'])
            download_uv += len(date_dict['download_uids'])
            collect_uv += len(date_dict['collect_uids'])
            play_song_cnt += len(date_dict['play_songs'])
            download_song_cnt += len(date_dict['download_songs'])
            collect_song_cnt += len(date_dict['collect_songs'])
        info_dict['play_pv'] = play_pv
        info_dict['download_pv'] = download_pv
        info_dict['collect_pv'] = collect_pv
        info_dict['play_uv'] = play_uv
        info_dict['download_uv'] = download_uv
        info_dict['collect_uv'] = collect_uv
        info_dict['play_song_cnt'] = play_song_cnt
        info_dict['download_song_cnt'] = download_song_cnt
        info_dict['collect_song_cnt'] = collect_song_cnt
        info_dict['play_pv_daily'] = play_pv / total_days
        info_dict['download_pv_daily'] = download_pv / total_days if total_days else 0
        info_dict['collect_pv_daily'] = collect_pv / total_days if total_days else 0
        info_dict['play_uv_daily'] = play_uv / total_days if total_days else 0
        info_dict['download_uv_daily'] = download_uv / total_days if total_days else 0
        info_dict['collect_uv_daily'] = collect_uv / total_days if total_days else 0
        info_dict['play_song_cnt_daily'] = play_song_cnt / total_days if total_days else 0
        info_dict['download_song_cnt_daily'] = download_song_cnt / total_days if total_days else 0
        info_dict['collect_song_cnt_daily'] = collect_song_cnt / total_days if total_days else 0
    logger.info('End process_user_actions')

    return artist_dict


def output(artist_dict, output_path):
    '''
    Args:
        artist_dict: dict, key:artist_id, value:{}
        output_path: string
    '''
    logger.info('Start output')
    col_names = ['song_num', 'avg_publish_cycle', 'avg_song_init_plays', 'l0', 'l1', 'l2', 'l3', 'l4', 'l5', 'gender',
                 'play_pv', 'download_pv', 'collect_pv', 'play_uv', 'download_uv', 'collect_uv', 'play_song_cnt', 'download_song_cnt', 'collect_song_cnt',
                 'play_pv_daily', 'download_pv_daily', 'collect_pv_daily', 'play_uv_daily', 'download_uv_daily', 'collect_uv_daily', 'play_song_cnt_daily', 'download_song_cnt_daily', 'collect_song_cnt_daily']
    with open(output_path, 'w') as fopen:
        fopen.write(','.join(col_names) + '\n')
        for artist_id, info_dict in artist_dict.iteritems():
            out_line = [artist_id]
            for col in col_names:
                out_line.append(str(info_dict[col]))
            fopen.write(','.join(out_line) + '\n')
    logger.info('End output')
    return output_path


def main():
    out_file = '%s/feature/artist_profile.csv' % (abs_father_path)

    artist_dict = process_artist_songs()
    artist_dict = process_user_actions(artist_dict)
    output(artist_dict, out_file)
    

if __name__ == '__main__':
    main()

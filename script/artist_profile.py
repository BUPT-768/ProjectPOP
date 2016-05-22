# -*- coding: utf-8 -*-
'''
Generate artist profile

Author: lujiaying
CreateDate: 2016/05/12
UpdateDate: 2016/05/22
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
from utils.basic_configs import ActionMap, TotalDays
from utils.pandas_utils import normalize_max_min

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
                artist_dict[artist_id] = {'song_num':0, 'publish_time_list':[], 'song_init_plays_sum':0, 'gender':gender}
                for i in range(101):
                    l_key = 'l%s' % (i)
                    artist_dict[artist_id][l_key] = 0
            artist_dict[artist_id]['song_num'] += 1
            artist_dict[artist_id]['publish_time_list'].append(publish_time)
            artist_dict[artist_id]['song_init_plays_sum'] += int(song_init_plays)
            l_key = 'l%s' % (language)
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
        ### 统计歌曲语言占比
        max_language, max_language_song_cnt = '0', 0
        language_cnt = 0
        for i in range(101):
            l_key = 'l%s' % (i)
            info_dict['l%s_rate'%(i)] = info_dict[l_key] / info_dict['song_num']
            if info_dict[l_key] > max_language_song_cnt:
                max_language_song_cnt = info_dict[l_key]
                max_language = str(i) 
            if info_dict[l_key] > 0:
                language_cnt += 1
        info_dict['favor_language'] = l_key
        info_dict['is_multi_language'] = 1 if language_cnt else 0
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
            ## 分日期统计
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
        # 绝对值特征
        ## 交互数
        info_dict['play_pv'] = play_pv
        info_dict['download_pv'] = download_pv
        info_dict['collect_pv'] = collect_pv
        ## 交互用户数
        info_dict['play_uv'] = play_uv
        info_dict['download_uv'] = download_uv
        info_dict['collect_uv'] = collect_uv
        ## 交互歌曲数
        info_dict['play_song_cnt'] = play_song_cnt
        info_dict['download_song_cnt'] = download_song_cnt
        info_dict['collect_song_cnt'] = collect_song_cnt
        ## 日均
        info_dict['play_pv_daily'] = play_pv / total_days
        info_dict['download_pv_daily'] = download_pv / total_days if total_days else 0
        info_dict['collect_pv_daily'] = collect_pv / total_days if total_days else 0
        info_dict['play_uv_daily'] = play_uv / total_days if total_days else 0
        info_dict['download_uv_daily'] = download_uv / total_days if total_days else 0
        info_dict['collect_uv_daily'] = collect_uv / total_days if total_days else 0
        info_dict['play_song_cnt_daily'] = play_song_cnt / total_days if total_days else 0
        info_dict['download_song_cnt_daily'] = download_song_cnt / total_days if total_days else 0
        info_dict['collect_song_cnt_daily'] = collect_song_cnt / total_days if total_days else 0
        ## 交互天数
        info_dict['day_actioned'] = total_days
        # 比值特征
        info_dict['day_actioned_rate'] = total_days / TotalDays
        info_dict['play_song_cnt_rate_daily'] = info_dict['play_song_cnt_daily'] / info_dict['song_num']
        info_dict['download_song_cnt_rate_daily'] = info_dict['download_song_cnt_daily'] / info_dict['song_num']
        info_dict['collect_song_cnt_rate_daily'] = info_dict['collect_song_cnt_daily'] / info_dict['song_num']
    logger.info('End process_user_actions')

    return artist_dict


def output(artist_dict, output_path):
    '''
    Args:
        artist_dict: dict, key:artist_id, value:{}
        output_path: string
    '''
    logger.info('Start output')
    col_names = ['song_num', 'avg_publish_cycle', 'avg_song_init_plays', 'favor_language', 'is_multi_language', 'gender',
                 'play_pv', 'download_pv', 'collect_pv', 'play_uv', 'download_uv', 'collect_uv', 'play_song_cnt', 'download_song_cnt', 'collect_song_cnt',
                 'play_pv_daily', 'download_pv_daily', 'collect_pv_daily', 'play_uv_daily', 'download_uv_daily', 'collect_uv_daily', 'play_song_cnt_daily', 'download_song_cnt_daily', 'collect_song_cnt_daily',
                 'day_actioned', 'day_actioned_rate', 'play_song_cnt_rate_daily', 'download_song_cnt_rate_daily', 'collect_song_cnt_rate_daily']
    for i in range(101):
        col_names.append('l%s' % (i))
        col_names.append('l%s_rate' % (i))
    with open(output_path, 'w') as fopen:
        fopen.write('artist_id,' + ','.join(col_names) + '\n')
        for artist_id, info_dict in artist_dict.iteritems():
            out_line = [artist_id]
            for col in col_names:
                out_line.append(str(info_dict[col]))
            fopen.write(','.join(out_line) + '\n')
    logger.info('End output, store in %s' % (output_path))
    return output_path

def preprocess_feature(source_file, output_file):
    '''
    数据预处理，主要包括归一化 和 特征选择

    Args:
        source_file: string
        output_file: string

    Returns:
        output_file
    '''
    logger.info('Start preprocess_feature')
    df = pandas.read_csv(source_file)
    # 归一化
    for col_name in ['song_num', 'avg_publish_cycle', 'avg_song_init_plays', 'play_pv_daily', 'play_uv_daily']:
        df = normalize_max_min(df, col_name)

    # 特征选择
    df_select = df[['artist_id', 'gender', 'favor_language', 'is_multi_language', 'song_num_normalized', 'avg_publish_cycle_normalized', 'avg_song_init_plays_normalized', 'play_pv_daily_normalized', 'play_uv_daily_normalized', 'play_song_cnt_rate_daily']]
    #logger.info(df_select.columns)
    df_select.to_csv(output_file, index=False)
    logger.info('Success preprocess_feature, store in %s' % (output_file))
    return output_file


def main():
    out_file = '%s/feature/artist_profile.csv' % (abs_father_path)

    artist_dict = process_artist_songs()
    artist_dict = process_user_actions(artist_dict)
    output(artist_dict, out_file)

    # 对原始特征离散化
    preprocess_feature(out_file, out_file.replace('artist_profile', 'artist_profile_normalized'))
    

if __name__ == '__main__':
    main()

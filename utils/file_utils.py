# coding=utf-8
from basic_configs import PROJECT_PATH
import pandas as pd

__author__ = 'jayvee', 'jiaying.lu'


def load_csv_as_dict(csv_path):
    """
    读取原始的csv数据文件，输出dict。此函数是一个生成器。
    :param csv_path:
    :return:
    """
    with open(csv_path, 'r') as fin:
        first_line = next(fin)
        titles = first_line.strip().split(',')
        datas = []
        for line in fin:
            tmp_dict = {}
            items = line.strip().split(',')
            for i in xrange(len(titles)):
                tmp_dict[titles[i]] = items[i]  # not using eval() in case of datetime data
            yield tmp_dict


def get_song_artist_map():
    '''
    获得一个 song_id=>artist_id的映射

    Returns:
        song_artist_map: dict, key,value均为string
    '''
    song_artist_map = {}
    with open('%s/data_source/mars_tianchi_songs.csv' % (PROJECT_PATH)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            song_id, artist_id = line_list[0], line_list[1]
            song_artist_map[song_id] = artist_id
    return song_artist_map

def get_all_artist_id():
    '''
    获得一个包含全部 artist_id的set

    Returns:
        artist_set: set, set of string
    '''
    artist_set = set()
    with open('%s/data_source/mars_tianchi_songs.csv' % (PROJECT_PATH)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            artist_id = line_list[1]
            if artist_id not in artist_set:
                artist_set.add(artist_id)
    return artist_set

def get_user_profile_dict():
    '''
    获得用户画像的dict

    Returns:
        user_profile_dict: dict, e.g. {uid1: {'key1':xx, 'key2':xx}, uid2: {}}
    '''
    user_profile_dict = {}
    with open('%s/feature/user_analysis_normalize.csv' % (PROJECT_PATH)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            user_id = line_list[0]
            user_profile_dict[user_id] = {}
            for i, key in enumerate(['user_play_cnt','user_play_days','user_play_cycle','user_play_songs','user_play_songs_daily','user_play_songs_daily','user_play_times_daily','top1_language','is_multi_language','p1_gender','p2_gender','p3_gender']):
                if key == 'top1_language':
                    continue
                user_profile_dict[user_id][key] = line_list[i+1]
                
    return user_profile_dict

def get_artist_profile_dict():
    '''
    获得艺人画像的dict

    Returns:
        artist_profile_dict: dict, e.g. {uid1: {'key1':xx, 'key2':xx}, uid2: {}}
    '''
    artist_profile_dict = {}
    with open('%s/feature/artist_profile_normalized.csv' % (PROJECT_PATH)) as fopen:
        fopen.readline()
        for line in fopen:
            line_list = line.strip().split(',')
            artist_id = line_list[0]
            artist_profile_dict[artist_id] = {}
            for i, key in enumerate(['gender','favor_language','is_multi_language','song_num_normalized','avg_publish_cycle_normalized','avg_song_init_plays_normalized','play_pv_daily_normalized','play_uv_daily_normalized','play_song_cnt_rate_daily']):
                artist_profile_dict[artist_id][key] = line_list[i+1]
    return artist_profile_dict


def test_main():
    res = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_songs.csv'))
    print len(res)


if __name__ == '__main__':
    test_main()
    # print(get_song_artist_map())

# coding=utf-8
import datetime
import pandas as pd
import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

from utils.basic_configs import timer
from utils.pandas_utils import str2datetime
from utils.log_tool import feature_logger

__author__ = 'jayvee'

# global 
Uid_aid_date_dict = {}
Uid_date_dict = {}
Aid_date_dict = {}

@timer
def get_pandas_usd_obj(user_artist_daily_path='%s/feature/user_artist_daily_stat.csv' % project_path):
    """
    load csv and return pandas obj
    :param user_artist_daily_path:
    :return:
    """
    daily_stat = pd.read_csv(user_artist_daily_path)
    # print 'complete loading csv to pandas'
    # transform str to datetime
    daily_stat['datetime'] = pd.to_datetime(daily_stat['date_str'], infer_datetime_format=True, format='%Y%m%d')
    df = daily_stat
    return df


def calc_user_artist_date_vec(user_id, artist_id, date_str, df, **tmp_dict):
    """
    The main function to calculate single user-song-date vector, contains some sub-functions.
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :return: features dict, e.g.{'key1':value1,'key2':value2,....}
    """
    feature_logger.info('Enter calc_user_artist_date_vec')
        # --- numercial features ---
    # user-song
    # ——近期指定用户播放指定艺人的次数
    feature_logger.info('start cal recent uid, aid, date_str , plays')
    one_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=1)
    three_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=3)
    seven_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=7)
    feature_logger.info('End cal recent uid, aid, date_str , plays')
    # user
    # ——近期指定用户的播放总数
    feature_logger.info('start cal recent uid, date_str , plays')
    one_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=1)
    three_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=3)
    seven_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=7)
    feature_logger.info('end cal recent uid, date_str , plays')
    # artist
    # ——指定艺人近期被播放的次数
    one_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=1)
    three_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=3)
    seven_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=7)
    # ——所有艺人近期被播放的总数，中间变量
    one_all_a_plays = tmp_dict.get('one_all_a_plays', _calc_all_artists_plays(date_str, df, day_offset=1))
    three_all_a_plays = tmp_dict.get('three_all_a_plays', _calc_all_artists_plays(date_str, df, day_offset=3))
    seven_all_a_plays = tmp_dict.get('seven_all_a_plays', _calc_all_artists_plays(date_str, df, day_offset=7))
    # play days
    one_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=1)
    three_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=3)
    seven_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=7)
    # --- ratio features ---
    # ——指定艺人近期的热门程度
    one_a_pop_rate = one_a_plays / max(1, one_all_a_plays)
    three_a_pop_rate = three_a_plays / max(1, three_all_a_plays)
    seven_a_pop_rate = seven_a_plays / max(1, seven_all_a_plays)
    # u a play rate
    one_u_a_play_rate = float(one_u_a_plays) / max(1, one_u_plays)
    three_u_a_play_rate = float(three_u_a_plays) / max(1, three_u_plays)
    seven_u_a_play_rate = float(seven_u_a_plays) / max(1, seven_u_plays)
    # --- binary features ---
    is_download = _calc_is_download(user_id, artist_id, date_str, df)
    is_collect = _calc_is_collect(user_id, artist_id, date_str, df)
    res_dict = {'one_u_a_plays': one_u_a_plays, 'three_u_a_plays': three_u_a_plays, 'seven_u_a_plays': seven_u_a_plays,
                'one_u_plays': one_u_plays, 'three_u_plays': three_u_plays, 'seven_u_plays': seven_u_plays,
                'one_a_plays': one_a_plays, 'three_a_plays': three_a_plays, 'seven_a_plays': seven_a_plays,
                'one_a_pop_rate': one_a_pop_rate, 'three_a_pop_rate': three_a_pop_rate,
                'seven_a_pop_rate': seven_a_pop_rate,
                'one_u_a_plays_days': one_u_a_plays_days, 'three_u_a_plays_days': three_u_a_plays_days,
                'seven_u_a_plays_days': seven_u_a_plays_days,
                'one_u_a_play_rate': one_u_a_play_rate, 'three_u_a_play_rate': three_u_a_play_rate,
                'seven_u_a_play_rate': seven_u_a_play_rate,
                'is_download': is_download, 'is_collect': is_collect
                }
    return res_dict


def _get_uid_aid_records_plays(user_id, artist_id, start_datetime, end_datetime):
    '''
    :param user_id:
    :param artist_id:
    :param date_str:
    :return: plays: float
    '''
    global Uid_aid_date_dict
    plays = 0.0
    for cur_datetime, action_dict in Uid_aid_date_dict[user_id][artist_id].iteritems():
        #feature_logger.debug('cur_datetime:%s, user_id:%s, artist_id:%s' % (cur_datetime, user_id, artist_id))
        if cur_datetime >= start_datetime and cur_datetime < end_datetime:
            plays += action_dict['plays']
    return plays

def _get_uid_records_plays(user_id, start_datetime, end_datetime):
    '''
    :param user_id:
    :param date_str:
    :return: plays: float
    '''
    global Uid_date_dict
    plays = 0.0
    for cur_datetime, action_dict in Uid_date_dict[user_id].iteritems():
        if cur_datetime >= start_datetime and cur_datetime < end_datetime:
            plays += action_dict['plays']
    return plays

def _get_aid_records_plays(artist_id, start_datetime, end_datetime):
    '''
    :param artist_id:
    :param date_str:
    :return: plays: float
    '''
    global Aid_date_dict
    plays = 0.0
    for cur_datetime, action_dict in Aid_date_dict[artist_id].iteritems():
        if cur_datetime >= start_datetime and cur_datetime < end_datetime:
            plays += action_dict['plays']
    return plays


def _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=1):
    """
    calc user-song-date-dayoffset plays
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    '''
    # handle day plays
    uid_aid_records = df[
        (df.user_id == user_id) & (df.artist_id == artist_id) &
        (df.datetime >= start_datetime) & (df.datetime < cur_datetime)]

    uid_aid_plays = 0.0
    for play in uid_aid_records.plays:
        uid_aid_plays += play
    '''
    # new process to handle day plays
    uid_aid_plays = _get_uid_aid_records_plays(user_id, artist_id, start_datetime, cur_datetime)
    return uid_aid_plays


def _calc_user_plays(user_id, date_str, df, day_offset=1):
    """
    calc user-date-dayoffset plays
    :param user_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    '''
    # handle day plays
    uid_records = df[
        (df.user_id == user_id) &
        (df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    uid_plays = 0.0
    for play in uid_records.plays:
        uid_plays += play
    '''
    # new process handle day plays
    uid_plays = _get_uid_records_plays(user_id, start_datetime, cur_datetime)
    return uid_plays


def _calc_artist_plays(artist_id, date_str, df, day_offset=1):
    """
    calc artist-date-dayoffset plays
    :param artist_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    '''
    # handle day plays
    # feature_logger.info('start df')
    aid_records = df[
        (df.artist_id == artist_id) &
        (df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    aid_plays = 0.0
    # feature_logger.info('start for')
    for play in aid_records.plays:
        aid_plays += play
    # feature_logger.info('end df')
    '''
    # new process handle day plays
    #feature_logger.info('start _get_records')
    aid_plays = _get_aid_records_plays(artist_id, start_datetime, cur_datetime)
    #feature_logger.info('end _get_records')
    return aid_plays


def _calc_artist_pop_rate(artist_id, date_str, df, day_offset=1, all_artist_plays=None):
    """
    calc recent artist_plays/all_artist_plays
    :param artist_id:
    :param date_str:
    :param df:
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    # get all_artist_plays if none
    if not all_artist_plays:
        all_artist_plays = _calc_all_artists_plays(date_str, df, day_offset)
    # handle day plays
    aid_records = df[
        (df.artist_id == artist_id) &
        (df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    aid_plays = 0.0
    for play in aid_records.plays:
        aid_plays += play
    return aid_plays / all_artist_plays


def _calc_all_artists_plays(date_str, df, day_offset=1):
    """
    calc recent all artists plays sum
    :param date_str:
    :param df:
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    all_artist_plays = 0.0
    all_a_records = df[(df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    for aplay in all_a_records.plays:
        all_artist_plays += aplay
    return all_artist_plays


def _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=1):
    """
    calc user-song-date plays during days
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :param day_offset:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    start_datetime = str2datetime(date_str) - datetime.timedelta(days=day_offset)
    '''
    # handle day plays
    uid_aid_records = df[(df.plays > 0) &
                         (df.user_id == user_id) & (df.artist_id == artist_id) &
                         (df.datetime >= start_datetime) & (df.datetime < cur_datetime)]
    '''
    days = 0
    for key_datetime, action_dict in Uid_aid_date_dict[user_id][artist_id].iteritems():
        if key_datetime >= start_datetime and key_datetime < cur_datetime:
            if action_dict['plays'] > 0:
                days += 1
    return float(days)


def _calc_is_download(user_id, artist_id, date_str, df):
    """
    check if a user has download the particular song before date_str
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    uid_aid_records = df[(df.user_id == user_id) & (df.artist_id == artist_id)
                         & (df.datetime < cur_datetime) & (df.downloads > 0)]
    return 1.0 if len(uid_aid_records) > 0 else 0.0


def _calc_is_collect(user_id, artist_id, date_str, df):
    """
    check if a user has collect the particular song before date_str
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df:
    :return:
    """
    cur_datetime = str2datetime(date_str)
    uid_aid_records = df[(df.user_id == user_id) & (df.artist_id == artist_id)
                         & (df.datetime < cur_datetime) & (df.collects > 0)]
    return 1.0 if len(uid_aid_records) > 0 else 0.0

def _load_file_to_dict():
    """
    读取原始的csv数据文件，输出dict
    """
    global Uid_aid_date_dict
    global Uid_date_dict
    global Aid_date_dict
    with open('%s/feature/user_artist_daily_stat.csv' % (project_path)) as fin:
        fin.readline()
        line_cnt = 0
        for line in fin:
            line_cnt += 1
            if line_cnt % 100000 == 0:
                feature_logger.info('_load_file_to_dict current stage: %s line' % (line_cnt))
            user_id,artist_id,date_str,plays,downloads,collects = line.strip().split(',')
            Ds_datetime = str2datetime(date_str)
            # Uid_aid_date_dict generate
            if user_id not in Uid_aid_date_dict:
                Uid_aid_date_dict[user_id] = {}
            if artist_id not in Uid_aid_date_dict[user_id]:
                Uid_aid_date_dict[user_id][artist_id] = {}
            if Ds_datetime not in Uid_aid_date_dict[user_id][artist_id]:
                Uid_aid_date_dict[user_id][artist_id][Ds_datetime] = {}
            Uid_aid_date_dict[user_id][artist_id][Ds_datetime] = {'plays':eval(plays), 'downloads':eval(downloads), 'collects':eval(collects)}

            # Uid_date_dict generate
            if user_id not in Uid_date_dict:
                Uid_date_dict[user_id] = {}
            if Ds_datetime not in Uid_date_dict[user_id]:
                Uid_date_dict[user_id][Ds_datetime] = {}
            Uid_date_dict[user_id][Ds_datetime] = {'plays':eval(plays), 'downloads':eval(downloads), 'collects':eval(collects)}

            # Aid_date_dict generate
            if artist_id not in Aid_date_dict:
                Aid_date_dict[artist_id] = {}
            if Ds_datetime not in Aid_date_dict[artist_id]:
                Aid_date_dict[artist_id][Ds_datetime] = {}
            Aid_date_dict[artist_id][Ds_datetime] = {'plays':eval(plays), 'downloads':eval(downloads), 'collects':eval(collects)}
    feature_logger.info('Uid_aid_date_dict len=%s' % (len(Uid_aid_date_dict)))
    return


def get_vectors_batch(paras_tuples, user_artist_daily_df):
    """
    生成器形式的用户x艺人x日期的vectorize
    :param paras_tuples: (user_id,artist_id,date_str)
    :param user_artist_daily_df: 读取了user_artist_daily_stat.csv(由daily_stat.py中的user_artist_daily_stats()方法生成)后的
           pandas DataFrame(由get_pandas_usd_obj()读取)
    :return:
    """
    # generate a dict
    for user_id, artist_id, date_str in paras_tuples:
        yield calc_user_artist_date_vec(user_id, artist_id, date_str, user_artist_daily_df)



if __name__ == '__main__':
    dframe = get_pandas_usd_obj()
    uid = '47786fe4a86082d8320ac3c07f34f7d9'
    aid = '3964ee41d4e2ade1957a9135afe1b8dc'
    dstr = '20150715'
    print _calc_is_download(uid, aid, dstr,
                            df=dframe)
    print _calc_user_artist_plays_days(uid, aid, dstr,
                                       day_offset=5, df=dframe)
    print _calc_user_artist_plays_days(uid, aid, dstr,
                                       day_offset=10, df=dframe)
    print _calc_user_artist_plays_days(uid, aid, dstr,
                                       day_offset=20, df=dframe)
    print _calc_user_artist_plays_days(uid, aid, dstr,
                                       day_offset=30, df=dframe)
    print _calc_user_artist_plays_days(uid, aid, dstr,
                                       day_offset=30, df=dframe)
    # calc_user_song_date_vec('1d1ae10bb6cd1c07ab7847b62614d144', 'f92ca5880075965aab7c0bcf59d781dc', '20150830', dframe)
    print calc_user_artist_date_vec(uid, aid, dstr, dframe)

import datetime
import pandas as pd
import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print project_path
sys.path.append(project_path)

from utils.basic_configs import timer
from utils.pandas_utils import str2datetime

__author__ = 'jayvee'


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


def calc_user_artist_date_vec(user_id, artist_id, date_str, df):
    """
    The main function to calculate user-song-date vector, contains some sub-functions.
    :param user_id:
    :param artist_id:
    :param date_str:
    :param df: pandas global DataFrame object
    :return:
    """
    # --- numercial features ---
    # user-song
    one_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=1)
    three_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=3)
    seven_u_a_plays = _calc_user_artist_plays(user_id, artist_id, date_str, df, day_offset=7)
    # user
    one_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=1)
    three_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=3)
    seven_u_plays = _calc_user_plays(user_id, date_str, df, day_offset=7)
    # artist
    one_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=1)
    three_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=3)
    seven_a_plays = _calc_artist_plays(artist_id, date_str, df, day_offset=7)
    # play days
    one_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=1)
    three_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=3)
    seven_u_a_plays_days = _calc_user_artist_plays_days(user_id, artist_id, date_str, df, day_offset=7)
    # --- ratio features ---
    # favor rate
    one_u_a_play_rate = float(one_u_a_plays) / max(1, one_u_plays)
    three_u_a_play_rate = float(three_u_a_plays) / max(1, three_u_plays)
    seven_u_a_play_rate = float(seven_u_a_plays) / max(1, seven_u_plays)
    # --- binary features ---
    is_download = _calc_is_download(user_id, artist_id, date_str, df)
    is_collect = _calc_is_collect(user_id, artist_id, date_str, df)
    return one_u_a_plays, three_u_a_plays, seven_u_a_plays, \
           one_u_plays, three_u_plays, seven_u_plays, \
           one_a_plays, three_a_plays, seven_a_plays, \
           one_u_a_plays_days, three_u_a_plays_days, seven_u_a_plays_days, \
           one_u_a_play_rate, three_u_a_play_rate, seven_u_a_play_rate, \
           is_download, is_collect


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
    # handle day plays
    uid_aid_records = df[
        (df.user_id == user_id) & (df.artist_id == artist_id) &
        (df.datetime >= start_datetime) & (df.datetime < cur_datetime)]
    uid_aid_plays = 0.0
    for play in uid_aid_records.plays:
        uid_aid_plays += play
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
    # handle day plays
    uid_records = df[
        (df.user_id == user_id) &
        (df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    uid_plays = 0.0
    for play in uid_records.plays:
        uid_plays += play
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
    # handle day plays
    aid_records = df[
        (df.artist_id == artist_id) &
        (df.datetime > start_datetime) & (df.datetime < cur_datetime)]
    aid_plays = 0.0
    for play in aid_records.plays:
        aid_plays += play
    return aid_plays


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
    # handle day plays
    uid_aid_records = df[(df.plays > 0) &
                         (df.user_id == user_id) & (df.artist_id == artist_id) &
                         (df.datetime >= start_datetime) & (df.datetime < cur_datetime)]
    return len(uid_aid_records)


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
    return 1 if len(uid_aid_records) > 0 else 0


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
    return 1 if len(uid_aid_records) > 0 else 0


if __name__ == '__main__':
    dframe = get_pandas_usd_obj()
    uid = '47786fe4a86082d8320ac3c07f34f7d9'
    aid = '3964ee41d4e2ade1957a9135afe1b8dc'
    dstr = '20150803'
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

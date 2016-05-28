# coding=utf-8
import datetime
import os
import sys
import collections
import pyspark
from operator import add

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

from utils.log_tool import feature_logger as logger

__author__ = 'jiaying.lu'

def _generate_song_artist_dict(line):
    '''
    Args:
        line: string
    Returns:
        song_artist_tuple: tuple, (song_id, artist_id)
    '''
    line_list = line.strip().split(',')
    return [line_list[0], line_list[1]]


def _add_artist_into_line(line, song_artist_dict_broadcast):
    '''
    给原文件添加一列 artist_id

    Args:
        line: string
        song_artist_dict_broadcast: pyspark.Broadcast
    Returns:
        line_result: string
    '''
    line_list = line.strip().split(',')
    song_id = line_list[1]
    artist_id = song_artist_dict_broadcast.value[song_id]
    return line+','+artist_id

def generate_user_actions_with_artist(sc):
    '''
    data_source/user_actions.csv 添加一列artist_id

    Args:
        sc: pyspark.SparkContext
    '''
    hdfs_file_dir = 'hdfs:/home/ProjectPOP/data_source'
    hdfs_song_path = '%s/mars_tianchi_songs.csv' % (hdfs_file_dir)
    hdfs_action_path = '%s/mars_tianchi_user_actions.csv' % (hdfs_file_dir)

    logger.info('Start generate song_artist_dict')
    song_artist_dict = dict(sc.textFile(hdfs_song_path).map(_generate_song_artist_dict).collect())
    song_artist_dict_broadcast = sc.broadcast(song_artist_dict)

    logger.info('Start process user_actions')
    user_actions = sc.textFile(hdfs_action_path).map(lambda l: _add_artist_into_line(l, song_artist_dict_broadcast))
    logger.info(user_actions.take(5))        
    user_actions.saveAsTextFile('%s/mars_tianchi_songs_with_artist.csv' % (hdfs_file_dir))
    return True


def _fixup_date(ua_dp_tuple):
    '''
    填补缺失的日期

    Args:
        ua_dp_tuple: tuple, e.g. ((user,artist), [(date1,play_cnt1),(date2,play_cnt2),()])
    Returns:
        ua_dp_tuple_fixup: tuple, e.g. ((user,artist), [(date1,play_cnt1),(date2,play_cnt2),()])
    '''
    start_day_Ymd = '20150301'
    end_day_Ymd = '20150830'
    start_day = datetime.datetime.strptime(start_day_Ymd, '%Y%m%d')
    end_day = datetime.datetime.strptime(end_day_Ymd, '%Y%m%d')

    ua_dp_key = ua_dp_tuple[0]
    ua_dp_value_dict = dict(ua_dp_tuple[1])
    for i in range((end_day-start_day).days+1):
        cur_day_Ymd = (start_day + datetime.timedelta(i)).strftime('%Y%m%d')
        if cur_day_Ymd not in ua_dp_value_dict:
            ua_dp_value_dict[cur_day_Ymd] = 0
    ua_dp_value = sorted(ua_dp_value_dict.items(), key=lambda t:int(t[0]))
    return (ua_dp_key, ua_dp_value)


def generate_user_play_table(sc):
    '''
    生成用户play记录表
    csv: user_id,artist_id,date,play_cnt

    Args:
        sc: pyspark.SparkContext
    '''
    hdfs_file_dir = 'hdfs:/home/ProjectPOP/data_source'
    hdfs_action_path = '%s/mars_tianchi_songs_with_artist.csv' % (hdfs_file_dir)

    logger.info('Start process user_actions')
    # 根据源文件生成csv，key=(user,artist,date),存在日期缺失的问题
    # result = [((user,artist,date),play_cnt), ()]
    user_actions = sc.textFile(hdfs_action_path).map(lambda l: l.split(',')) \
            .filter(lambda l: False if l[0]=='user_id' or l[3]!='1' else True) \
            .map(lambda l: ((l[0],l[5],l[4]), 1)) \
            .reduceByKey(add)
    logger.info('user_actions: %s' % (user_actions.take(5)))

    # 补全key=(user,artist) 对应的180天内的play_cnt
    logger.info('Start complete user_actions')
    user_actions_completed = user_actions.map(lambda l: ((l[0][0],l[0][1]),(l[0][2],l[1]))) \
            .groupByKey() \
            .map(_fixup_date) \
            .flatMapValues(lambda x:x)
    logger.info('user_actions_completed: %s' % (user_actions_completed.take(5)))
    # 输出文件: user_id,artist_id,date,play_cnt
    logger.info('Output result')
    user_actions_completed.map(lambda l: '%s,%s,%s,%s' % (l[0][0], l[0][1], l[1][0], l[1][1])) \
            .saveAsTextFile('%s/user_artist_date_play.csv' % (hdfs_file_dir))
    return True


if __name__ == '__main__':
    import platform
    logger.info('python version: %s' % (platform.python_version()))

    logger.info('Start init pyspark conf')
    conf = pyspark.SparkConf().setAppName('generate_feature')
    sc = pyspark.SparkContext(conf=conf)

    #generate_user_actions_with_artist(sc)
    generate_user_play_table(sc)

    sc.stop()

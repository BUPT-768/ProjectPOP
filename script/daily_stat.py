# coding=utf-8
from collections import defaultdict
import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print project_path
sys.path.append(project_path)
from utils.basic_configs import PROJECT_PATH
from utils.file_utils import load_csv_as_dict

__author__ = 'jayvee'
'''
本脚本用于生成最原始的daily统计数据，包含了对齐前的统计和对齐后的统计。
'''


def actions_stats(user_out_name, song_out_name):
    """
    actions的基本预处理，统计出每天的播放、下载、收藏量
    :param user_out_name: 以user为基准的统计文件名，不包含.csv
    :param song_out_name: 以song为基准的统计文件名，不包含.csv
    :return:
    """
    actions = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_user_actions.csv'))
    user_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0)))  # 默认dict的一个trick！
    song_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0)))
    user_song_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0)))
    # user_dict = defaultdict(dict)
    # 统计行为
    count = 0
    for act in actions:
        user_id = act['user_id']
        song_id = act['song_id']
        action_type = act['action_type']
        # gmt_create = eval(act['gmt_create'])
        # date_create = Arrow.fromtimestamp(gmt_create)
        # date_str = date_create.format('YYYY-MM-DD')
        date_str = act['Ds']
        user_dict[user_id][date_str][action_type] += 1
        song_dict[song_id][date_str][action_type] += 1
        count += 1
        if count % 10000 == 0:
            print 'handled %s records' % count
    print 'total users:%s' % len(user_dict)
    print 'total songs:%s' % len(song_dict)
    # 输出结果到feature文件夹中
    with open('%s/feature/%s.csv' % (PROJECT_PATH, user_out_name), 'w') as user_out, \
            open('%s/feature/%s.csv' % (PROJECT_PATH, song_out_name), 'w') as song_out:
        user_out.write('user_id,date_str,plays,downloads,favors\n')
        count = 0
        for user_id in user_dict.keys():
            user_act = user_dict[user_id]
            for date_str in user_act.keys():
                acts = user_act[date_str]
                user_out.write('%s,%s,%s,%s,%s\n' % (user_id, date_str, acts['1'], acts['2'], acts['3']))
            count += 1
            progress = 100.0 * count / float(len(user_dict))
            if progress % 5 == 0:
                print 'users out %s done' % progress
        print 'user actions stats done.'
        song_out.write('song_id,date_str,plays,downloads,favors\n')
        count = 0
        for song_id in song_dict.keys():
            song_act = song_dict[song_id]
            for date_str in song_act.keys():
                acts = song_act[date_str]
                song_out.write('%s,%s,%s,%s,%s\n' % (song_id, date_str, acts['1'], acts['2'], acts['3']))
            count += 1
            progress = 100.0 * count / float(len(song_dict))
            if progress % 5 == 0:
                print 'songs out %s done' % progress
        print 'user actions stats done.'


def user_song_stats(user_song_out_name):
    """
    对user和song的每日播放、下载、收藏数据进行对齐处理，统计出每天的播放、下载、收藏量
    :param user_song_out_name: 以user-song为key的统计文件名，不包含.csv
    :return:
    """
    actions = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_user_actions.csv'))
    user_song_dict = defaultdict(lambda: defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0))))
    # 统计行为
    count = 0
    for act in actions:
        user_id = act['user_id']
        song_id = act['song_id']
        action_type = act['action_type']
        date_str = act['Ds']
        user_song_dict[user_id][song_id][date_str][action_type] += 1
        count += 1
        if count>10:
            break
        if count % 10000 == 0:
            print 'handled %s records' % count
    # print 'total users:%s' % len(user_dict)
    # print 'total songs:%s' % len(song_dict)
    # 输出结果到feature文件夹中
    with open('%s/feature/%s.csv' % (PROJECT_PATH, user_song_out_name), 'w') as user_song_out:
        user_song_out.write('user_id,song_id,date_str,plays,downloads,favors\n')
        count = 0
        for user_id in user_song_dict.keys():
            for song_id in user_song_dict[user_id].keys():
                user_act = user_song_dict[user_id][song_id]
                for date_str in user_act.keys():
                    acts = user_act[date_str]
                    user_song_out.write(
                        '%s,%s,%s,%s,%s,%s\n' % (user_id, song_id, date_str, acts['1'], acts['2'], acts['3']))
                count += 1
                progress = 100.0 * count / float(len(user_song_dict))
                if progress % 5 == 0:
                    print 'user-song out %s done' % progress
        print 'user-song actions stats done.'


if __name__ == '__main__':
    # actions_stats('user_actions_stats', 'song_actions_stats')
    user_song_stats('user_song_daily_stat')

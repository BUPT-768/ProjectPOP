# coding=utf-8
from collections import defaultdict
import os
from arrow import Arrow
import sys


project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(project_path)
print project_path

from utils.file_utils import load_csv_as_dict
from utils.basic_configs import PROJECT_PATH

__author__ = 'zhouxiaojiang'

'''
action处理隔3天，7天，30天的user表的统计数据，包括user的行为分析，和歌曲的行为分析
input：间隔的天数，user表的输出位置，song表的输出位置
return：user_id/song_id,date_interval(以3-15为基准的间隔interval的整数倍)，行为统计
'''
def user_action_stat(interval,user_out_name,song_out_name):
    #统计用户三日的数据量
    ## step 1: 读入数据
    base_time = 1426348800 #3-15-0-0-0的unix时间戳
    base_time_stamp = Arrow.fromtimestamp(base_time)
    interval_seconds = interval * 24 * 3600
    parts = load_csv_as_dict('%s/data_source/%s' %(PROJECT_PATH,'mars_tianchi_user_actions.csv'))
    user_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0)))  # 默认dict的一个trick！
    song_dict = defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:0.0)))
    count = 0
    ## step 2:统计数据
    for part in parts:
        user_id = part['user_id']
        song_id = part['song_id']
        action_type = part['action_type']
        gmt_create = eval(part['gmt_create'])
        date_interval_belong = int((Arrow.fromtimestamp(gmt_create) - base_time_stamp).total_seconds())/interval_seconds
        user_dict[user_id][date_interval_belong][action_type] += 1
        song_dict[song_id][date_interval_belong][action_type] += 1
        count += 1
        if count % 1000 == 0:
            print 'statistical %s records' % count
    print 'total users: %s' % len(user_dict)
    print 'total songs: %s' % len(song_dict)
    ## step 3:写入到feature文件
    fs = open('%s/feature/%s.csv' % (PROJECT_PATH,user_out_name),'w')
    fs.write('user_id,date_interval_%s ,plays,downloads,favors\n' % interval)
    count = 0
    for user in user_dict:
        date_dict = user_dict[user]
        for date in date_dict:
            action = date_dict[date]
            fs.write('%s,%s,%s,%s,%s\n' % (user,date,action['1'],action['2'],action['3']))
            count = count + 1
            if count % 1000 == 0:
                print 'write %s length' % count
    fs.close()
    print 'user_dict is write done'
    fs = open('%s/feature/%s.csv' % (PROJECT_PATH,song_out_name),'w')
    fs.write('song_id,date_interval_%s,plays,downloads,favors\n' % interval)
    count = 0
    for song in song_dict:
        date_dict = song_dict[song]
        for date in date_dict:
            action = date_dict[date]
            fs.write('%s,%s,%s,%s,%s\n' % (song,date,action['1'],action['2'],action['3']))
            count += 1
            if count % 1000 == 0:
                print 'write %s length' % count
    fs.close()
    print 'song_dict is write done'


if __name__=='__main__':
    user_action_stat(30,'user_stat_30','song_stat_30')


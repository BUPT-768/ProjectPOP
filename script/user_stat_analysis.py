# -*- coding: utf-8 -*-

'''
User basic statistical

Author:zhouxiaojiang
Date:2016/05/17
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

def user_analysis():
    
    #用户维度的统计
    #step 1 读入数据
    logger.info('start read csv')
    user_dict = {}
    fs = open('%s/mars_tianchi_user_actions.csv' % data_source_dir,'r')
    fs.readline()
    for line in fs:
        line_list = line.strip().split(',')
        user_id,song_id,action_type,Ds = line_list[0],line_list[1],eval(line_list[3]),line_list[4]
        if not user_dict.has_key(user_id):
            user_dict[user_id] = {'song_total_num':0,'song_play':0,'song_download':0,'song_favor':0,'listen_time_list':[],'play':0,'download':0,'favor':0}
        user_dict[user_id]['song_num'] += 1
        user_dict[user_id]['listen_time_list'].append(Ds)
        if action_type == 1:
            user_dict[user_id]['play'] += 1
        elif action_type == 2:
            user_dict[user_id]['download'] += 1
        elif action_type == 3:
            user_dict[user_id]['favor'] += 1
    fs.close()
    logger.info('end read csv')
    logger.info('start cal ')
    #step 2 计算
    for user_id,info_dict in user_dict.iteritems():
        #用户平均每首歌的统计
        info_dict['average_song_play'] = info_dict['play'] / info_dict['song_num']
        info_dict['average_song_download'] = info_dict['download'] / info_dict['song_num']
        info_dict['average_song_favor'] = info_dict['favor'] / info_dict['song_num']
        listen_time_list = [l for l in info_dict['listen_time_list']]
        span_time = 1
        start_date,end_date = min(listen_time_list),max(listen_time_list)
        if start_date != end_date:
            start_datetime = datetime.datetime.strptime(str(start_date),'%Y%m%d')
            end_datetime = datetime.datetime.strptime(str(end_date),'%Y%m%d')
            span_time = (end_datetime - start_datetime).days
        #用户听歌的时间跨度
        info_dict['span_time'] = span_time
        #用户平均每天的统计
        info_dict['average_time_play'] = info_dict['play'] / span_time
        info_dict['average_time_download'] = info_dict['download'] / span_time
        info_dict['average_time_favor'] = info_dict['favor'] / span_time
    logger.info('end cal')
    logger.info('start write')
    #step 3 输出
    fs = open('%s/feature/user_analysis.csv' % abs_father_path,'w' )
    fs.write('用户id，用户听歌数量,用户听歌时间跨度，用户每首歌的平均播放数，用户每首歌的平均下载数,用户每首歌的平均收藏数,用户平均每天播放数，用户平均每天下载数，用户平均每天收藏数\n')
    for user_id,info_dict in user_dict.iteritems():
        fs.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (user_id,info_dict['song_num'],info_dict['span_time'],info_dict['average_song_play'],info_dict['average_song_download'],info_dict['average_song_favor'],info_dict['average_time_play'],info_dict['average_time_download'],info_dict['average_time_favor']))
    fs.close()
    logger.info('end write')
if __name__ == '__main__':
    user_analysis()



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
from collections import defaultdict

abs_path = os.path.dirname(os.path.abspath(__file__))
abs_father_path = os.path.dirname(abs_path)
sys.path.append(abs_father_path)

from utils.log_tool import feature_logger as logger

data_source_dir = '%s/data_source' % (abs_father_path)

def user_analysis():
    #用户维度的统计:
    #step 1 读入数据
    logger.info('start read csv')
    user_dict = {}
    fs = open('%s/mars_tianchi_user_actions.csv' % data_source_dir,'r')
    fs.readline()
    for line in fs:
        line_list = line.strip().split(',')
        user_id,song_id,action_type,Ds = line_list[0],line_list[1],eval(line_list[3]),line_list[4]
        if not user_dict.has_key(user_id):
            user_dict[user_id] = {'song_id_list':[],'song_days':set(),'song_set':set(),'play':0,'download':0,'favor':0,'play_days':set(),'download_days':set(),'favor_days':set(),'play_song':set(),'download_song':set(),'favor_song':set()}
        user_dict[user_id]['song_id_list'].append(song_id)
        user_dict[user_id]['song_set'].add(song_id)
        user_dict[user_id]['song_days'].add(Ds)
        if action_type == 1:
            user_dict[user_id]['play'] += 1
            user_dict[user_id]['play_days'].add(Ds)
            user_dict[user_id]['play_song'].add(song_id)
        elif action_type == 2:
            user_dict[user_id]['download'] += 1
            user_dict[user_id]['download_days'].add(Ds)
            user_dict[user_id]['download_song'].add(song_id)
        elif action_type == 3:
            user_dict[user_id]['favor'] += 1
            user_dict[user_id]['favor_days'].add(Ds)
            user_dict[user_id]['favor_song'].add(song_id)
    fs.close()
    logger.info('end read csv')
    return user_dict

def song_analysis():
    '''
    歌曲维度的统计
    '''
    logger.info('start read csv')
    fs = open('%s/mars_tianchi_songs.csv' %  data_source_dir,'r')
    song_dict = {}
    fs.readline()
    for line in fs:
        line_list = line.strip().split(',')
        song_id,artist_id,Language,gender = line_list[0],line_list[1],eval(line_list[4]),eval(line_list[5])
        if not song_dict.has_key(song_id):
            song_dict[song_id] = {'artist_id':'','gender':0,'Language':0}
        song_dict[song_id]['artist_id'] = artist_id 
        song_dict[song_id]['gender'] = gender
        song_dict[song_id]['Language'] = Language
    fs.close()
    logger.info('end read csv')
    return song_dict       
    
def user_song_analysis():    
    '''
    用户歌曲维度的统计
    '''

    song_dict = song_analysis()
    user_dict = user_analysis()

    logger.info('start cal ')
    #step 2 计算
    for user_id,info_dict in user_dict.iteritems():
        #用户平均每首歌的统计

        info_dict['song_num'] = len(info_dict['song_id_list'])
        info_dict['song_num_uniq'] = len(info_dict['song_set'])
        info_dict['song_days_num'] = len(info_dict['song_days'])
        info_dict['song_interval_frequency'] = _cal_song_interval_frequency(info_dict['song_days'])
        info_dict['play_time'] = info_dict['play']
        info_dict['download_time'] = info_dict['download']
        info_dict['favor_time'] = info_dict['favor']
        info_dict['play_day_num'] = len(info_dict['play_days'])
        info_dict['play_interval_frequency'] = _cal_song_interval_frequency(info_dict['play_days'])
        info_dict['download_day_num'] = len(info_dict['download_days'])
        info_dict['download_interval_frequency'] = _cal_song_interval_frequency(info_dict['download_days'])
        info_dict['favor_day_num'] = len(info_dict['favor_days'])
        info_dict['favor_interval_frequency'] = _cal_song_interval_frequency(info_dict['favor_days'])
        info_dict['play_song_num'] = len(info_dict['play_song'])
        info_dict['download_song_num'] = len(info_dict['download_song'])
        info_dict['favor_song_num'] = len(info_dict['favor_song'])
        song_id_list = [song_id for song_id in info_dict['song_id_list']]
        top1,top2,top3,top4,top5 = _top5_language(song_id_list,song_dict)

        p1,p2,p3 = _gender_prob(song_id_list,song_dict)
        info_dict['top1_language'] = top1
        info_dict['top2_language'] = top2
        info_dict['top3_language'] = top3
        info_dict['top4_language'] = top4
        info_dict['top5_language'] = top5
        info_dict['p1_gender'] = p1
        info_dict['p2_gender'] = p2
        info_dict['p3_gender'] = p3
        if info_dict['play_day_num'] != 0:
            info_dict['average_day_play_song'] = info_dict['play_song_num'] / info_dict['play_day_num']
            info_dict['average_day_play_times'] = info_dict['play_time'] / info_dict['play_day_num']
        else:
            info_dict['average_day_play_song'] = 0
            info_dict['average_day_play_times'] = 0
        if info_dict['download_day_num'] != 0:    
            info_dict['average_day_download_song'] = info_dict['download_song_num'] / info_dict['download_day_num']
            info_dict['average_day_download_times'] = info_dict['download_time'] / info_dict['download_day_num']
        else:
            info_dict['average_day_download_song'] = 0
            info_dict['average_day_download_times'] = 0
        if  info_dict['favor_day_num'] != 0 :
            info_dict['average_day_favor_song'] = info_dict['favor_song_num'] / info_dict['favor_day_num']
            info_dict['average_day_favor_times'] = info_dict['favor_time'] / info_dict['favor_day_num']
        else:
            info_dict['average_day_favor_song'] = 0
            info_dict['average_day_favor_times'] = 0
    logger.info('end cal')
    logger.info('start write')
    #step 3 输出
    fs = open('%s/feature/user_analysis.csv' % abs_father_path,'w' )

    fs.write('用户id,用户行为数量,用户行为歌曲数,用户行为天数,用户行为间隔天数,用户播放次数,用户下载次数,用户收藏次数,用户播放天数,用户下载的天数,用户下载间隔天数,用户收藏的天数,用户收藏间隔天数,用户播放的歌曲数,用户下载的歌曲数,用户收藏的歌曲数,用户平均每天播放歌曲数,用户平均每天下载歌曲数,用户平均每天收藏歌曲书,用户平均每天播放次数,用户平均每天下载次数,用户平均每天收藏次数,top1_language,top2_language,top3_language,top4_language,top5_language,p1_gender,p2_gender,p3_gender\n')
    for user_id,info_dict in user_dict.iteritems():
        fs.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (user_id,info_dict['song_num'],info_dict['song_num_uniq'],info_dict['song_days_num'],info_dict['song_interval_frequency'],info_dict['play_time'],info_dict['download_time'],info_dict['favor_time'],info_dict['play_day_num'],info_dict['play_interval_frequency'],info_dict['download_day_num'],info_dict['download_interval_frequency'],info_dict['favor_day_num'],info_dict['favor_interval_frequency'],info_dict['play_song_num'],info_dict['download_song_num'],info_dict['favor_song_num'],info_dict['average_day_play_song'],info_dict['average_day_download_song'],info_dict['average_day_favor_song'],info_dict['average_day_play_times'],info_dict['average_day_download_times'],info_dict['average_day_favor_times'],info_dict['top1_language'],info_dict['top2_language'],info_dict['top3_language'],info_dict['top4_language'],info_dict['top5_language'],info_dict['p1_gender'],info_dict['p2_gender'],info_dict['p3_gender']))
    fs.close()
    logger.info('end write')

def _top5_language(song_id_list,song_dict):
    song_language_dict = defaultdict(lambda:0)
    top_5_language = []
    for song_id in song_id_list:
        language = song_dict[song_id]['Language']
        song_language_dict[language] += 1
    favor_language = sorted(song_language_dict.iteritems(),key = lambda d:d[1],reverse = True)
    if len(favor_language) >= 5:
        favor_language = favor_language[0:5]
    for l in favor_language:
        top_5_language.append(l[0])
    #补充不足top5的情况，用-1增加
    if len(top_5_language) < 5:
        for i in range((5 - len(top_5_language))):
            top_5_language.append(-1)
    return top_5_language    
    
def _gender_prob(song_id_list,song_dict):
   #统计个数
    song_gender_dict = defaultdict(lambda:0)
    for song_id in song_id_list:
        gender = int(song_dict[song_id]['gender'])
        song_gender_dict[gender] += 1
   #计算gender的概率
    total = len(song_id_list)
    p1 = song_gender_dict[1] / total
    p2 = song_gender_dict[2] / total
    p3 = song_gender_dict[3] / total
    return (p1,p2,p3)

def _cal_song_interval_frequency(song_date_list):
    song_date_list = list(song_date_list)
   # sorted(song_date_list)
    song_date_list.sort()
    logger.info('the sorted list %s' % song_date_list)
    if len(song_date_list) == 0:
        return -1
    elif len(song_date_list) == 1:
        return 1
    date_interval_list = []    
    start_date = song_date_list[0]
    for i in range(1,len(song_date_list)):
        next_date = song_date_list[i]
        start_date_datetime = datetime.datetime.strptime(start_date,'%Y%m%d')
        next_date_datetime = datetime.datetime.strptime(next_date,'%Y%m%d')
        date_interval_list.append((next_date_datetime - start_date_datetime).days)
        start_date = next_date
    sum_num = 0
    for interval in date_interval_list:
        sum_num += interval
    mean_interval = sum_num / len(date_interval_list)
   # if mean_interval < 0:
   #     logger.info('the error list %s ' % song_date_list)
    return mean_interval

if __name__ == '__main__':
    user_song_analysis()



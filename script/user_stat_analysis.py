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
from utils.basic_configs import TotalDays
from utils.pandas_utils import normalize_max_min

import numpy
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
        top1,is_multi_language = _top5_language(song_id_list,song_dict)

        p1,p2,p3 = _gender_prob(song_id_list,song_dict)
        info_dict['top1_language'] = top1
        info_dict['is_multi_language'] = is_multi_language
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

    fs.write('user_id,user_song_cnt,user_song_unique,user_days_cnt,user_action_cycle,user_play_cnt,user_download_cnt,user_collect_cnt,user_play_days,user_play_cycle,user_download_days,user_download_cycle,user_collect_days,user_collect_cycle,user_play_songs,user_download_songs,user_collect_songs,user_play_songs_daily,user_downlaod_songs_daily,user_collect_songs_daily,user_play_times_daily,user_download_times_daily,user_collect_times_daily,top1_language,is_multi_language,p1_gender,p2_gender,p3_gender\n')
    for user_id,info_dict in user_dict.iteritems():
        fs.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (user_id,info_dict['song_num'],info_dict['song_num_uniq'],info_dict['song_days_num'],info_dict['song_interval_frequency'],info_dict['play_time'],info_dict['download_time'],info_dict['favor_time'],info_dict['play_day_num'],info_dict['play_interval_frequency'],info_dict['download_day_num'],info_dict['download_interval_frequency'],info_dict['favor_day_num'],info_dict['favor_interval_frequency'],info_dict['play_song_num'],info_dict['download_song_num'],info_dict['favor_song_num'],info_dict['average_day_play_song'],info_dict['average_day_download_song'],info_dict['average_day_favor_song'],info_dict['average_day_play_times'],info_dict['average_day_download_times'],info_dict['average_day_favor_times'],info_dict['top1_language'],info_dict['is_multi_language'],info_dict['p1_gender'],info_dict['p2_gender'],info_dict['p3_gender']))
    fs.close()
    logger.info('end write')

def _top5_language(song_id_list,song_dict):
    if len(song_id_list) == 0:
        return (-1,0)
    is_multi_language = 0
    song_language_dict = defaultdict(lambda:0)
    top_5_language = []
    for song_id in song_id_list:
        language = song_dict[song_id]['Language']
        song_language_dict[language] += 1
    favor_language = sorted(song_language_dict.iteritems(),key = lambda d:d[1],reverse = True)
    if len(favor_language) >= 2:
        is_multi_language = 1
    return (favor_language[0][0],is_multi_language)    
    
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
#    logger.info('the sorted list %s' % song_date_list)
    if len(song_date_list) == 0:
        return -1
    elif len(song_date_list) == 1:
        return 1
    song_date_list.sort()
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
    if mean_interval < 0:
        logger.info('the error list %s ' % song_date_list)
    return mean_interval

def preprocess_feature(source_file,otput_file ):
    '''
    数据预处理，主要包括归一化和特征选择

    Args:
        source_file: string
        output_file: string
    Returns:
        output_file
    '''
    logger.info('Start preprocess_feature')
    df = pandas.read_csv(source_file)

    #数值型 归一化
    for col_name in ['user_play_cnt','user_play_cycle','user_play_songs','user_play_songs_daily','user_play_times_daily']:
        df = normalize_max_min(df,col_name,1)
    #日期型 归一化
 #   df = df['user_play_days'] / TotalDays
    
    df = df['user_play_days'] /  TotalDays
    #特征选择
    df_select = df[['user_id','user_play_cnt','user_play_days','user_play_cycle','user_play_songs','user_play_songs_daily','user_play_times_daily','top1_language','is_multi_language','p1_gender','p2_gender','p3_gender']]
    df_select.to_csv(output_file,mode= 'w' ,index = False)
    logger.info('Success preProcess ,store in %s' % (output_file))
    return output_file

def _one_hot(top1_language):
    language_vec = [0] * 100 


if __name__ == '__main__':
  #  user_song_analysis()
   source_file = '%s/feature/user_analysis.csv' % (abs_father_path)
   output_file = source_file.replace('user_analysis','user_analysis_normalize')
   preprocess_feature(source_file,output_file) 
    


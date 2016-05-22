# coding = utf-8

from collections import defaultdict
import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

from utils.basic_configs import PROJECT_PATH
from utils.file_utils import load_csv_as_dict

__author__ = 'zhouxiaojiang'

def artist_song():
    actions = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH,'mars_tianchi_songs.csv'))
    artist_dict = defaultdict(list)
    for action in actions:
        song_id = action['song_id']
        artist_id = action['artist_id']
        artist_dict[artist_id].append(song_id)
    return artist_dict    
#    print len(artist_dict)    

def song_action():
    actions = load_csv_as_dict('%s/feature/%s' % (PROJECT_PATH,'song_actions_stats.csv'))
    song_dict = defaultdict(lambda:defaultdict(list))
    for action in actions:
        song_id = action['song_id']
        date_str = action['date_str']
#        print date_str
        plays = eval(action['plays'])
        downloads = eval(action['downloads'])
        favors = eval(action['favors'])
        temp_dict = {}
        if plays > 0:
            temp_dict[date_str] = plays
            song_dict[song_id]['plays'].append((temp_dict))
#            print (song_dict[song_id]['plays'])
        if downloads > 0:
            temp_dict[date_str] = downloads
            song_dict[song_id]['downloads'].append(temp_dict)
        if favors > 0:
            temp_dict[date_str] = favors    
            song_dict[song_id]['favors'].append(temp_dict)
    return song_dict

def artist_action():
    artist_dict = artist_song()
    song_dict = song_action()
    artist_action_dict = defaultdict(lambda:defaultdict(lambda:defaultdict(dict)))
    for artist in artist_dict:
        song_list = artist_dict[artist]
        for song in song_list:
            song_action_dict = song_dict[song]
            artist_action_dict[artist][song] = song_action_dict
    #print artist_action_dict

    ## statistical
    print 'start write file'
    fs = open('%s/feature/%s' % (PROJECT_PATH,'artist_action.csv'),'w')
    fs.write('artist_id,song_id,date,plays,downloads,favors\n')
    
    for artist in artist_action_dict:
        for song in artist_action_dict[artist]:
            play_list = artist_action_dict[artist][song]['plays']
            downloads_list = artist_action_dict[artist][song]['downloads']
            favors_list =artist_action_dict[artist][song]['favors']
            for play in play_list:
                fs.write('%s,%s,%s,%s,%s,%s' % (artist,song,play.keys(),paly.))

            fs.write('%s,%s,%s,%s,%s\n' % (artist,song,play_list,downloads_list,favors_list))
            
    fs.close()
    print 'end write file'

if __name__ == '__main__':
    artist_action()


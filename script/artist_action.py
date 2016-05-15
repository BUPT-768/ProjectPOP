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
        if plays > 0:
            song_dict[song_id]['plays'].append((date_str,plays))
            print (song_dict[song_id]['plays'])
        if downloads > 0:
            song_dict[song_id]['downloads'].append((date_str,downloads))
        if favors > 0:
            song_dict[song_id]['favors'].append((date_str,favors))
    for k in song_dict:
        actin = song_dict[k]
 #       print type(action)
      #  print list(action['plays'])
      #  print '%s id %s plays %s date' % (k,action['plays'][0][0],action['plays'][0][1])
if __name__ == '__main__':
    song_action()


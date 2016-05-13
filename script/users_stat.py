# coding=utf-8
from collections import defaultdict
from arrow import Arrow
from utils.basic_configs import PROJECT_PATH
from utils.file_utils import load_csv_as_dict
from utils.log_tool import model_logger

__author__ = 'jayvee'


def user_actions_stats(fout_name):
    actions = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_user_actions.csv'))
    user_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: 0.0)))  # 默认dict的一个trick！
    # user_dict = defaultdict(dict)
    # 统计行为
    count = 0
    for act in actions:
        user_id = act['user_id']
        gmt_create = eval(act['gmt_create'])
        action_type = act['action_type']
        date_create = Arrow.fromtimestamp(gmt_create)
        date_str = date_create.format('YYYY-MM-DD')
        user_dict[user_id][date_str][action_type] += 1
        count += 1
        if count % 10000 == 0:
            print 'handled %s records' % count
    print 'total users:%s' % len(user_dict)
    # 输出结果到feature文件夹中
    with open('%s/feature/%s.csv' % (PROJECT_PATH, fout_name), 'w') as fout:
        fout.write('user_id,date_str,plays,downloads,favors\n')
        count = 0
        for user_id in user_dict.keys():
            user_act = user_dict[user_id]
            for date_str in user_act.keys():
                acts = user_act[date_str]
                fout.write('%s,%s,%s,%s,%s\n' % (user_id, date_str, acts['1'], acts['2'], acts['3']))
            count += 1
            progress = 100.0 * count / float(len(user_dict))
            if progress % 5 == 0:
                print '%s done' % progress
        print 'user actions stats done.'


if __name__ == '__main__':
    user_actions_stats('user_actions_stats')

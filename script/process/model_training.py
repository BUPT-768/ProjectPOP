# coding=utf-8
import os
import sys

cur_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(os.path.dirname(cur_path))
sys.path.append(project_path)

from script.user_artist_date_vectorize import calc_user_artist_date_vec, get_pandas_usd_obj, _calc_all_artists_plays
from utils.file_utils import load_csv_as_dict
from utils.log_tool import model_logger

__author__ = 'jayvee'


def __dict2vec(input_dict):
    """
    inner function to transform dict type to list type
    :param input_dict:
    :return: a list
    """
    if isinstance(input_dict, dict):
        tmp = []
        for value in input_dict.values():
            tmp.append(value)
        return tmp
    else:
        raise TypeError('input type should be a dict!')


def get_vectors_by_label_infos(label_infos):
    """
    获取模型输入的vecs
    :param label_infos: list, [(label, user_id, artist_id, date_str)]
    :return:
    """

    dframe = get_pandas_usd_obj()
    vec_list = []
    label_list = []
    for label, user_id, artist_id, date_str in label_infos:
        tmp_vec = get_vector_by_single_info(user_id, artist_id, date_str, dframe)
        yield tmp_vec

        # label_list.append(label)
        # vec_list.append(tmp_vec)
        # return label_list, vec_list


def get_vector_by_single_info(user_id, artist_id, date_str, dframe, **tmp_params):
    tmp_vec = []
    tmp_dict = {}
    recent_feature_dict = calc_user_artist_date_vec(user_id, artist_id, date_str, dframe, **tmp_params)
    # TODO user_profile
    user_profile_dict = {}
    # todo artist profile
    artist_profile_dict = {}
    try:
        recent_feature_vec = __dict2vec(recent_feature_dict)
        user_profile_vec = __dict2vec(user_profile_dict)
        artist_profile_vec = __dict2vec(artist_profile_dict)
        tmp_dict = recent_feature_dict.copy()
        tmp_dict.update(user_profile_dict)
        tmp_dict.update(artist_profile_dict)
        tmp_vec.extend(recent_feature_vec)
        tmp_vec.extend(user_profile_vec)
        tmp_vec.extend(artist_profile_vec)
    except TypeError, te:
        model_logger.error('%s,%s error, details=%s' % (user_id, artist_id, te))
    return tmp_vec, tmp_dict


def traversal_daily_labels():
    """
    遍历feature/dailyFeature/下的所有文件并获取label_infos, 同时save合并后的feature

    :return:
    """
    print project_path
    dframe = get_pandas_usd_obj()
    file_tree = os.walk('%s/feature/dailyFeature' % project_path)
    d = next(file_tree)
    daily_base_path = d[0]
    datestrs = d[1]
    # 获取各vector的生成指标名称
    titles = ['label', 'user_id', 'artist_id', 'date_str']
    tmp_path = daily_base_path + os.path.sep + datestrs[0] + os.path.sep + 'train_label.csv'
    tmp_in = open(tmp_path, 'r')
    next(tmp_in)
    tmp_line = next(tmp_in)
    tmp_items = tmp_line.strip().split(',')
    tmp_vec, tmp_dict = get_vector_by_single_info(tmp_items[0], tmp_items[1], tmp_items[2], dframe)
    tkeys = tmp_dict.keys()
    tkeys.sort()
    titles.extend(tkeys)
    # 开始正式遍历
    for date_str in datestrs:
        filepath = daily_base_path + os.path.sep + date_str + os.path.sep + 'train_label.csv'
        fout_path = daily_base_path + os.path.sep + date_str + os.path.sep + 'train_vec.csv'
        model_logger.info('handling %s' % filepath)
        one_all_a_plays = _calc_all_artists_plays(date_str, dframe, day_offset=1)
        three_all_a_plays = _calc_all_artists_plays(date_str, dframe, day_offset=3)
        seven_all_a_plays = _calc_all_artists_plays(date_str, dframe, day_offset=7)
        with open(fout_path, 'w') as fout:
            first_line = ','.join(titles)
            fout.write(first_line + '\n')
            count = 0
            for label_info in load_csv_as_dict(filepath):
                label = label_info['label']
                user_id = label_info['user_id']
                artist_id = label_info['artist_id']
                date_str = label_info['date']
                tmp_vec, tmp_dict = get_vector_by_single_info(user_id, artist_id, date_str, dframe,
                                                              one_all_a_plays=one_all_a_plays,
                                                              three_all_a_plays=three_all_a_plays,
                                                              seven_all_a_plays=seven_all_a_plays)
                tmp_dict['label'] = label
                tmp_dict['user_id'] = user_id
                tmp_dict['artist_id'] = artist_id
                tmp_dict['date_str'] = date_str
                line_str_list = [str(tmp_dict[k]) for k in titles]
                line = ','.join(line_str_list) + '\n'
                fout.write(line)
                count += 1
                if count % 1000:
                    model_logger.info('%s has completed %s vectors.' % (date_str, count))
                    fout.flush()
                    # yield tmp_vec, tmp_dict


# def get_label_info(csv_path):
#     with open(csv_path, 'r') as fin:
#         titles = next(fin)
#         for line in fin:
#             features = line.strip().split(',')
#             user_id = features[0]
#             artist_id = features[1]
#             date_str


def train_model(vec_list, mod_path):
    pass


if __name__ == '__main__':
    traversal_daily_labels()

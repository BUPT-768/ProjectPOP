# coding=utf-8
import os
import sys
from utils.log_tool import model_logger

cur_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(cur_path)
sys.path.append(project_path)

from script.user_artist_date_vectorize import calc_user_artist_date_vec, get_pandas_usd_obj

__author__ = 'jayvee'


def get_vectors_by_label_infos(label_infos):
    """
    获取模型输入的vecs
    :param label_infos: list, [(label, user_id, artist_id, date_str)]
    :return:
    """

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

    dframe = get_pandas_usd_obj()
    vec_list = []
    label_list = []
    for label, user_id, artist_id, date_str in label_infos:
        tmp_vec = []
        recent_feature_dict = calc_user_artist_date_vec(user_id, artist_id, date_str, dframe)
        # TODO user_profile
        user_profile_dict = {}
        # todo artist profile
        artist_profile_dict = {}
        try:
            recent_feature_vec = __dict2vec(recent_feature_dict)
            user_profile_vec = __dict2vec(user_profile_dict)
            artist_profile_vec = __dict2vec(artist_profile_dict)
            tmp_vec.extend(recent_feature_vec)
            tmp_vec.extend(user_profile_vec)
            tmp_vec.extend(artist_profile_vec)
        except TypeError, te:
            model_logger.error('%s,%s error, details=%s' % (user_id, artist_id, te))
            continue
        label_list.append(label)
        vec_list.append(tmp_vec)
    return label_list, vec_list


def train_model(vec_list, mod_path):
    pass


if __name__ == '__main__':
    a = ('aaaa', 2, {"dsad": False})

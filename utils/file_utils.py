# coding=utf-8
from utils.basic_configs import PROJECT_PATH

__author__ = 'jayvee'


def load_csv_as_dict(csv_path):
    """
    读取原始的csv数据文件，输出dict。此函数是一个生成器。
    :param csv_path:
    :return:
    """
    with open(csv_path, 'r') as fin:
        first_line = next(fin)
        titles = first_line.strip().split(',')
        datas = []
        for line in fin:
            tmp_dict = {}
            items = line.strip().split(',')
            for i in xrange(len(titles)):
                tmp_dict[titles[i]] = items[i]  # not using eval() in case of datetime data
            yield tmp_dict


def test_main():
    res = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_songs.csv'))
    print len(res)


if __name__ == '__main__':
    test_main()

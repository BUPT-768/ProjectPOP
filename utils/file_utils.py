from utils.basic_configs import PROJECT_PATH

__author__ = 'jayvee'


def load_csv_as_dict(csv_path):
    with open(csv_path, 'r') as fin:
        first_line = next(fin)
        titles = first_line.replace('\n', '').split(',')
        datas = []
        for line in fin:
            tmp_dict = {}
            items = line.replace('\n', '').split(',')
            for i in xrange(len(titles)):
                tmp_dict[titles[i]] = items[i]  # not using eval() in case of datetime data
            datas.append(tmp_dict)
        return datas


def test_main():
    res = load_csv_as_dict('%s/data_source/%s' % (PROJECT_PATH, 'mars_tianchi_songs.csv'))
    print len(res)


if __name__ == '__main__':
    test_main()

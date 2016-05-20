import os
import arrow

__author__ = 'jayvee'

"""
store some configs of the project
"""
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ActionMap = {'1':'play', '2':'download', '3':'collect'}
TotalDays = 183
PositiveLabel = 1
NegativeLabel = 0

def timer(func):
    def wrapper(*args, **kwargs):
        t = arrow.utcnow()
        res = func(*args, **kwargs)
        print '[%s]cost time = %s' % (func.__name__, arrow.utcnow() - t)
        return res

    return wrapper



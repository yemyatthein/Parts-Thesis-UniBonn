import calendar
from datetime import datetime, timedelta
from config import DATE_FORMAT, TIME_FORMAT

def start_end_hour(dt):
    firstday = dt.replace(minute=0, second=0, microsecond=0)
    lastday = dt.replace(minute=59, second=0, microsecond=0)
    return firstday, lastday

def start_end_day(dt):
    firstday = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    lastday = dt.replace(hour=23, minute=59, second=0, microsecond=0)
    return firstday, lastday
    
def start_end_week(dt):
    current = dt
    wd = dt.weekday()
    while wd > 0:
        current -= timedelta(1)
        wd = current.weekday()
    return current.replace(hour=0, minute=0, second=0, microsecond=0), \
        (current + timedelta(6)).replace(hour=23, minute=59, second=0, microsecond=0)
    
def start_end_month(dt):
    ld = calendar.monthrange(dt.year, dt.month)[1]
    firstday = datetime.strptime('%s-%s-01 00:00:00' % (dt.year, dt.month), \
                '%s %s' % (DATE_FORMAT, TIME_FORMAT))
    lastday = datetime.strptime('%s-%s-%s 23:59:00' % (dt.year, dt.month, ld), \
                '%s %s' % (DATE_FORMAT, TIME_FORMAT))
    return firstday, lastday

def start_end_year(dt):
    ld = calendar.monthrange(dt.year, dt.month)[1]
    firstday = datetime.strptime('%s-01-01 00:00:00' % dt.year, \
                '%s %s' % (DATE_FORMAT, TIME_FORMAT))
    lastday = datetime.strptime('%s-12-31 23:59:00' % dt.year, \
                '%s %s' % (DATE_FORMAT, TIME_FORMAT))
    return firstday, lastday

gran_to_func = {'h': start_end_hour,
                'd': start_end_day,
                'w': start_end_week,
                'm': start_end_month,
                'y': start_end_year}
        
def period_to_hashkey(username, start, end, additional=[]):
    if start.hour == end.hour:
        startkey = '%s/%s' % (start.date(), start.hour)
        endkey = '%s/%s' % (start.date(), (start.hour + 1))
    else:
        startkey = '%s/0' % str(start.date())
        endkey = '%s/23' % str(end.date())
    hashkey = '%s:%s|%s' % (username, startkey, endkey)
    for ax in additional:
        hashkey += ':%s' % ax
    return hashkey

def decode_time(start, end):
    stokens = start.split('/')
    etokens = end.split('/')
    stokens[1] = int(stokens[1])
    etokens[1] = int(etokens[1])
    sdate = datetime.strptime(stokens[0], DATE_FORMAT).replace(hour=int(stokens[1]))
    if stokens[0] == etokens[0] and etokens[1] == stokens[1] + 1:
        edate = datetime.strptime(etokens[0], DATE_FORMAT).replace(hour=int(stokens[1]), minute=59)
    else:
        edate = datetime.strptime(etokens[0], DATE_FORMAT).replace(hour=int(etokens[1]))
    return sdate, edate

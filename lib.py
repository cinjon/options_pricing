import urllib2
import time
import datetime
import pytz
import re

time_fastforward_sum = 0
second = 1000000 # We keep track of time in millionths of a second                                 
minute = 60 * second
hour = 60 * minute
day = 24 * hour
week = 7 * day
year = 365.25 * day
month = year / 12

month_number = {'January':'01', 'February':'02', 'March':'03', 'April':'04',
                'May':'05', 'June':'06', 'July':'07', 'August':'08', 
                'September':'09', 'October':'10', 'November':'11', 'December':'12'}
sleep_mod = 180 #150 works, 200 doesnt. 175 doesnt (fails at SATC)

def now():
    """Returns the current time in microseconds from the epoch time"""
    return int(time_fastforward_sum) + int(time.time()*1000*1000)

def month_to_day(month):
    return month_number[month]

def dt_to_time(dt):
    if dt == 'any':
        return None
    return long(time.mktime(time.strptime(dt, '%Y-%m-%d'))) * 1000 * 1000

def time_to_dt(t):
    '''YYYY-MM-DD of time; this is the format of the dt partitions in hive'''
    return time.strftime('%Y-%m-%d', time.gmtime(t / 1000. / 1000.))

def get_local_datetime(t):
    return datetime.datetime.fromtimestamp(t / 1000000).replace(tzinfo=pytz.utc).astimezone(
        pytz.timezone("America/Los_Angeles"))
#     return datetime.datetime.fromtimestamp(t / 1000000).astimezone(
#         current_tz() or pytz.timezone("America/Los_Angeles"))

def timestamp_of_time(t):
    d = get_local_datetime(t)
    return d.strftime("%b %d, %G %I:%M %p %Z")

def read_page(url):
    try:
        req = urllib2.Request(url)
        result = urllib2.urlopen(req)
        return result.read()
    except Exception, e:
        print url
        raise

def sleep(count):
    if count % sleep_mod == 0:
        print 'Sleeping'
        time.sleep(60*8.5)
        print 'Done sleeping'

def parse_tickerlist(fileloc):
    file = open(fileloc, 'r')
    lines = file.readlines()
    set_tickers = set()
    tickers = []
    pattern = re.compile(r'[A-Z]+$')
    for line in lines[1:]:
        ticker = line.split(',')[0]
        ticker = ticker.strip('"')
        result = pattern.search(ticker)
        if ticker not in set_tickers and result and result.group() == ticker:
            set_tickers.add(ticker)
            tickers.append(ticker)
    return tickers

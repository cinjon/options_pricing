import urllib2
import re
import lib
import db
from bs4 import BeautifulSoup
import random

host_prefix = 'http://finance.yahoo.com'
option_prefix = '/q/op?s='

def get_nasdaq_nyse_tickers():
    nasdaq_file = '/Users/cinjon/Desktop/options/nasdaq.csv'
    nyse_file = '/Users/cinjon/Desktop/options/nyse.csv'
    nasdaq = lib.parse_tickerlist(nasdaq_file)
    nyse = lib.parse_tickerlist(nyse_file)
    random.shuffle(nasdaq)
    random.shuffle(nyse)
    return nasdaq, nyse

def get_table_tickers():
    return db.retrieve_tickers_from_database(randomize=False)

def make_url(ticker):
    return (host_prefix + option_prefix + ticker.capitalize() + '+Options')

def _make_dates_regex(ticker):
    return (option_prefix + ticker + '&m=', re.compile(r'(\d{4})-(\d{2})$'))

def _regex_href(href, pattern):
    return pattern.search(href)

def get_dates(soup, ticker):
    ret = []
    pre, pattern = _make_dates_regex(ticker)
    for tag in soup.findAll(href=re.compile(ticker)):
        href = tag.get('href')
        if href and href[:len(pre)] == pre:            
            search = _regex_href(href[len(pre):], pattern)
            if search:
                ret.append(search.group())
    return ret

def _parse_month_day(st):
    parts = st.split(' ')
    return (lib.month_to_day(parts[0].strip()), parts[1].strip())

def _parse_expiration(i):
    parts = i.split(',')
    year = parts[2].strip()
    month, day = _parse_month_day(parts[1].strip())
    ret = (str(year) + '-' + month + '-' + day)
    return lib.dt_to_time(ret) + 13*lib.hour #Correcting for market close

def _find_expiration(td):
    for child in td.findAll('td'):
        if len(child.contents) > 0:
            contents = child.contents
            if 'Expire at' in contents[0]:
                return _parse_expiration(contents[0])
    return None

def numberize_dict(d):
    '''Turn select fields of the dict d into explicit numbers'''
    new_d = {}
    for ty,value in d.iteritems():
        if ty != 'symbol':
            new_d[ty] = str(value).replace(',', '')
        else:
            new_d[ty] = value
    return new_d
        
def _parse_pricing(tr):
    '''tr has 8 children by being in here'''
    tr = list(tr)
    return numberize_dict({'strike':tr[0].string, 'symbol':tr[1].string, 'last':tr[2].string,
                           'vol':tr[6].string, 'open':tr[7].string})

def _parse_td(td):
    calls = []
    puts = []
    call_flag = False
    expiration = _find_expiration(td)

    for child in td.findAll('tr'):
        if len(child.contents) == 8:
            if child.contents[0].string == 'Strike':
                call_flag = not call_flag
                continue

            pricing = _parse_pricing(child)
            pricing['expiration'] = expiration
            if call_flag:
                calls.append(pricing)
            else:
                puts.append(pricing)

    return calls, puts

def parse(soup):
    for td in soup.findAll('td'):
        if len(td.contents) > 0 and 'View By Expiration' in td.contents[0]:
            return _parse_td(td)
    return [], []
            
def get_option_info(date, ticker):
    url = host_prefix + option_prefix + ticker + '&m=' + date
    page = lib.read_page(url)
#     db.record_page_log(ticker, lib.now(), page)
    soup = BeautifulSoup(lib.read_page(url))        
    return parse(soup)

def record_options(calls, puts, ticker):
    now = lib.now()
    if len(calls) > 0:
        db.record_calls(calls, now, ticker)
    if len(puts) > 0:
        db.record_puts(puts, now, ticker)

def run_ticker(ticker):
    ret = 0
    url = make_url(ticker)
    soup = BeautifulSoup(lib.read_page(url))
    calls, puts = parse(soup)
    if len(calls) + len(puts) > 0:
        ret += 1
        record_options(calls, puts, ticker)
    for date in get_dates(soup, ticker):            
        calls, puts = get_option_info(date, ticker)
        if len(calls) + len(puts) > 0:
            ret += 1
            record_options(calls, puts, ticker)
    return ret
    
def run_nasdaq_nyse():
    nasdaq, nyse = get_nasdaq_nyse_data()
    time = lib.now()
    print 'Started Running: %s' % time
    count = 0
    print 'Nasdaq Commencing: %s' % lib.now()
    for ticker in nasdaq:
        print ticker
        run_ticker(ticker)
        count += 1
        lib.sleep(count)
    nasdaq_time = lib.now()
    print 'Nasdaq Finished, count: %s' % count
    print 'NYSE Commencing: %s' % lib.now()
    for ticker in nyse:
        print ticker
        run_ticker(ticker)
        count += 1
        lib.sleep(count)
    nyse_time = lib.now()
    print 'NYSE Finished, count: %s' % count
    db.close_db()
    print 'Finished. Timing for nasdaq: %s, Timing for nyse: %s' % (nasdaq_time - time, nyse_time - nasdaq_time)

def run_table_tickers():
    time = lib.now()
    tickers = get_table_tickers()
    time_ticker = lib.now()
    print 'Started Running: %s' % time_ticker
    count = 0
    pages_hit = 0
    reached = False
    try:
        for ticker in tickers:
            print ticker
            pages_hit += run_ticker(ticker)
            count += 1
            lib.sleep(count)
        db.close_db()
    finally:
        print 'Finished. Timing for getting tickers: %s, Timing for getting data: %s, Sleep count: %s, Pages Hit: %s' % (time_ticker - time, lib.now() - time_ticker, count/lib.sleep_mod, pages_hit)
    
def run():
    run_table_tickers()
#     run_nasdaq_nyse()

if __name__=="__main__":
    run()

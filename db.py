import MySQLdb

mysql_db = {}
def _connect_db(db='options', user='root', host='localhost'):
    this_db = mysql_db.get('db')
    if not this_db:
        try:
            this_db = MySQLdb.connect(db=db, user=user, host=host)
        except Exception, e:
            print 'in connecting'
            print e
    if this_db:
        mysql_db['db'] = this_db
        return (this_db, _cursor(this_db))
    return None

def _cursor(db):
    return db.cursor()

def close_db():
    db = mysql_db.get('db')
    if db:
        db.close()

def record_page_log(ticker, time, data):
    try:
        db, cursor = _connect_db()
        cursor.execute("INSERT INTO page_log (ticker, time, data) VALUES (%s, %s, %s)", 
                       (ticker, time, data))
        db.commit()
    except Exception, e:
        print e
        print data

def _record_options(options):
    try:
        db, cursor = _connect_db()
        cursor.executemany(
            """INSERT INTO pricing (ticker, time, type, expiration, symbol, strike, last, vol, open)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            options)
        db.commit()
    except Exception, e:
        print 'recording options'
        print e
        print options

def reformat_options(options, time, ticker, type):
    return [(ticker, time, type, 
             option.get('expiration'), option.get('symbol'), 
             option.get('strike'), option.get('last'), 
             option.get('vol'), option.get('open')) for option in options]

def record_calls(calls, time, ticker):
    reformatted_calls = reformat_options(calls, time, ticker, 'c')
    _record_options(reformatted_calls)
    
def record_puts(puts, time, ticker):
    reformatted_puts = reformat_options(puts, time, ticker, 'p')
    _record_options(reformatted_puts)

def retrieve_tickers_from_database(randomize=True):
    try:
        db, cursor = _connect_db()
        db.query('select distinct(ticker) as ticks from pricing order by ticks')
        uids = db.store_result().fetch_row(maxrows=0)
        uids = [u[0] for u in uids]
        if randomize:
            import random
            random.shuffle(uids)
        return uids
    except Exception, e:
        raise

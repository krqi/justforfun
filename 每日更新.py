import tushare as ts
from sqlalchemy import create_engine
import sqlite3
import pandas as pd
from datetime import datetime as dt

#每日更新
#注1：dt.now()是指今天，dt.now().weekday是返回今天是星期几，5代表星期六，6代表星期天。
#注2：today指的是最近的一个交易日，df.ix[-1].name是数据库中最新的一天，
#     if today != df.ix[-1].name[:10]意思就是，如果数据库最新的一天不是最近一个交易日，则要开始更新数据。
con = sqlite3.connect('History.db')
query1 = "select name from sqlite_master where type='table' order by name"
stocklist = pd.read_sql(query1, con).name

engine = create_engine('sqlite:///History.db', echo = False)

updatestock = []
for stock in stocklist:
    query2 = "select * from '%s' order by date" %stock
    df = pd.read_sql(query2, con)
    df = df.set_index('date')
    if dt.now().weekday() == 5: #注1
        today = str(pd.Timestamp(dt.now())-pd.Timedelta(days = 1))[:10]  #注2
    elif dt.now().weekday() == 6:
        today = str(pd.Timestamp(dt.now())-pd.Timedelta(days = 2))[:10] 
    else:
        today = str(pd.Timestamp(dt.now()))[:10]
    if today != df.ix[-1].name[:10]:
        try:
            df = ts.get_h_data(stock, start = df.ix[-1].name[:10], retry_count = 5)
            df.to_sql(stock, engine, if_exists='append')
            updatestock.append(stock)
        except:
            continue

f = open('updated.txt','w')
print >>f, updatestock
f.close()
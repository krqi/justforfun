import tushare as ts
import sqlite3
import pandas as pd #注2
import json 
from urllib.request import urlopen, Request
from sqlalchemy import create_engine
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

#准备工作
conn1=sqlite3.connect('ts_StocklistBase.db') #注4 Stocklist.db是存放股票列表的数据库
engine = create_engine('sqlite:///163rc.db', echo = False) #注5
conn2 = sqlite3 .connect ("163rc.db")
stocklist = []
errorlist = []
alreadylist = []

#获取股票列表并插入列表库
#stocklist = ts.get_stock_basics()
#stocklist.to_sql("Allist", conn1, if_exists="replace")

query1 = "select * from Allist" #注7 SQL语句。Allist是Stocklist.db中的表，存放股票列表的
stocklist = pd.read_sql(query1, conn1)

query2 = "select name from sqlite_master where type='table' order by name" #注10 意思是获取所有表名
alreadylist = pd.read_sql(query2, conn2) #注11 
print (alreadylist )
url = "http://img1.money.126.net/data/hs/klinederc/day/history/"#2015/0601318.json"

#----------------------------------------------------------------------
def down163rc(stock):
    """下载163历史交易数据"""
    code = stock[0] #[:6] #注1 stock取自上文的stocklist， 由于是tuple，含有两列，第1列取做code，第15列取作marketday，后者是该股票的上市日。
    marketday = stock[15] #注1    

    #600代码前加0,000代码前加1,300代码前加1(沪市是0，深市是1)
    if str(code)[0] == '6':
        precode = "0"
    else:
        precode = "1"
    if (int(marketday ) > 0) and (code not in list(alreadylist.name)): #还首发公司上市日是0
        for year in range(int(marketday/10000) ,2018):
            print("code = " +str(code) + ":" + str(year))
            try:
                realurl = url + str(year) + "/" + precode + str(code) + ".json"
                dr = urlopen(realurl,timeout= 30).read()
                js = json.loads(dr.decode('utf-8'))
                dt = pd.DataFrame(list(js['data']), columns= ['date','open','close','high','low','volume','percentage']) #单独解析价格部分
                dt.set_index ('date', inplace= True)
        
                f = pd.DataFrame (js)        
                dt['code'] = f["symbol"][0]
                dt["name"] = f["name"][0]
                dt.to_sql(code, engine, if_exists="append")
            except :
                errorlist.append(str(code) + "," + str(year))    
                print("ERROR:"+str(code)+":"+str(year))
        
#for s in stocklist.values:
    #down163rc(s)
pool = ThreadPool(8)
try:
    pool.map(down163rc, stocklist.values)
except:
    print("pool.map() except!"+ str(stocklist.shape))
    
pool.close()
pool.join()

fdf = pd.DataFrame(errorlist)
fdf.to_excel ("163rcsaved.xlsx")
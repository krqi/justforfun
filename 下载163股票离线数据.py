import tushare as ts
import sqlite3
import pandas as pd #注2
from urllib.request import urlopen, Request
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

#准备工作
conn1=sqlite3.connect('Stocklist.db') #注4 Stocklist.db是存放股票列表的数据库

stocklist = []
errorlist = []
alreadylist = []

#获取股票列表并插入列表库
#stocklist = ts.get_stock_basics()
#stocklist.to_sql("Allist", conn1, if_exists="replace")

query1 = "select * from Allist" #注7 SQL语句。Allist是Stocklist.db中的表，存放股票列表的
stocklist = pd.read_sql(query1, conn1)
print(stocklist.shape)

url = "http://quotes.money.163.com/service/chddata.html?code=" #0601398&start=20000720&end=20150508"

#----------------------------------------------------------------------
def down163(stock):
    """下载163历史交易数据"""
    code = stock[0] #[:6] #注1 stock取自上文的stocklist， 由于是tuple，含有两列，第1列取做code，第15列取作marketday，后者是该股票的上市日。
    marketday = stock[15] #注1    
    print(code)
    now = pd.datetime.now()#  ->这是时间数组格式
    today = now.strftime("%Y%m%d") #转换为指定的格式:
    #600代码前加0,000代码前加1,300代码前加1(沪市是0，深市是1)
    if str(code)[0] == '6':
        code = "0" + str(code)
    else:
        code = "1" + str(code)
    realurl = url + str(code) + "&start=" + str(marketday) + "&end=" + str(today)
    try:
        f = urlopen(realurl, timeout= 30)
        data = f.read() 
        with open("163/" + str(code[1:]) + ".csv", "wb") as f:     
            f.write(data)           
    except:
        errorlist.append(stock[0])    
        
#for s in stocklist.values:
    #down163(s)
pool = ThreadPool(8)
try:
    pool.map(down163, stocklist.values)
except:
    print("pool.map() except!"+ str(stocklist.shape))
    
pool.close()
pool.join()

fdf = pd.DataFrame(errorlist)
fdf.to_csv("Notsaved.txt")
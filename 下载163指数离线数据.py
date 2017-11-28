import tushare as ts
import sqlite3
import pandas as pd #注2
from urllib.request import urlopen, Request
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

#准备工作
conn1=sqlite3.connect('Stocklist.db') #注4 Stocklist.db是存放股票列表的数据库

indexlist = []
errorlist = []

#获取指数列表并插入列表库
#indexlist = ts.get_index()
#indexlist.to_sql("Indexlist", conn1, if_exists="replace")

query1 = "select * from Indexlist" #注7 SQL语句。Allist是Stocklist.db中的表，存放股票列表的
indexlist = pd.read_sql(query1, conn1)
print(indexlist.shape)
print(indexlist.iloc[0])

url = "http://quotes.money.163.com/service/chddata.html?code=" #0601398&start=20000720&end=20150508"

#----------------------------------------------------------------------
def down163index(stock):
    """下载163历史交易数据"""
    code = stock[1] #[:6] #注1 stock取自上文的stocklist， 由于是tuple，含有两列，第1列取做code，第15列取作marketday，后者是该股票的上市日。
    marketday = "19901219"#stock[15] #注1    
    print(code)
    now = pd.datetime.now()#  ->这是时间数组格式
    today = now.strftime("%Y%m%d") #转换为指定的格式:
    #000代码前加0,399代码前加1(沪市是0，深市是1)
    if str(code)[0] == '0':
        code = "0" + str(code)
    else:
        code = "1" + str(code)
        
    realurl = url + str(code) + "&start=" + str(marketday) + "&end=" + str(today)
    try:
        f = urlopen(realurl, timeout= 90)
        data = f.read() 
        with open("163index/" +str(code[1:]) + ".csv", "wb") as f:     
            f.write(data)           
    except:
        errorlist.append(stock[1])    
        
#for s in indexlist.values:
    #down163index(s)
pool = ThreadPool(8)
try:
    pool.map(down163index, indexlist.values)
except:
    print("pool.map() except!"+ str(indexlist.shape))

pool.close()
pool.join()

fdf = pd.DataFrame(errorlist)
fdf.to_csv("Notsaved.txt")
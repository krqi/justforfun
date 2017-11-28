import tushare as ts
import lxml
from sqlalchemy import create_engine #注1 sqlalchemy是Python自带的与数据库联结的包，导入创建数据库联结的函数
import sqlite3
import pandas as pd #注2
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

#准备工作
conn1=sqlite3.connect('Stocklist.db') #注4 Stocklist.db是存放股票列表的数据库
engine = create_engine('sqlite:///History.db', echo = False) #注5 创建与sqlite数据库的联结，名字为History.db
#获取股票列表并插入列表库
#stocklist = ts.get_stock_basics()
#stocklist.to_sql("Allist", conn1, if_exists="replace")

conn2 = sqlite3.connect('History.db')

stocklist = []
errorlist = []
alreadylist = []
query1 = "select * from Allist" #注7 SQL语句。Allist是Stocklist.db中的表，存放股票列表的
stocklist = pd.read_sql(query1, conn1)
conn1.close()
query2 = "select name from sqlite_master where type='table' order by name" #注10 意思是获取所有表名
alreadylist = pd.read_sql(query2, conn2) #注11 另一种读数据库的方法，直接用pandas读取数据库，alreadylist是DataFrame类型，有代码的那一列名为name

#获取数据并保存的函数
#利用tushare的get_h_data函数获取数据。
#由于会出现如网络错误或其他错误，导致该程序重新执行，所以必须验证以防止添加重复数据。
def save(stock):
    code = stock[0] #[:6] #注1 stock取自上文的stocklist， 由于是tuple，含有两列，第1列取做code，第15列取作marketday，后者是该股票的上市日。
    print(code)

    if code not in list(alreadylist.name): #注2 上文中用SQL语句查询出了一个DataFramealreadlist，包含了History.db数据库中已有的表名，用alreadlist.name取出，name是alreadlist的列名
        marketday = stock[15] #注1
        i= 0
        try:
            startday = pd.Timestamp(str(marketday)) # pd.Timestamp可以把文本类型的日期转成时间戳类型的，这样就可以进行时间的运算，例如通过pd.Timedelta。然后就照startday和enday的写法，三年一个跨度拉取数据。

            df = ts.get_h_data(code, start=str(startday)[:10], retry_count = 5)
            df = df.sort_index(ascending=True) #注4 数据拉过来是以date为索引的，但是还需要重新排序，因而这样写以升序排列。
            print(df.shape)
            ma_list = [5,10,20,30,60]
            for ma in ma_list:
                df['ma' + str(ma)] = df.close.rolling(ma).mean() #pd.rolling_mean(df.close, ma) #注5 没有移动均线的数据，因而手动计算。pandas直接自带移动平均数的计算函数pd.rolling_mean，两个参数分别是计算对象和计算参数
            df.to_sql(code, engine, if_exists='append') #注6 写入数据库，if_exists='append'意为追加的形式。
  
        except:
            errorlist.append(stock[0])
        #注7 用try...except的方式来避免异常中断，错误的股票写入errorlist，最后程序结束时打印出来

#for s in stocklist.values:
    #save(s)
#多线程处理
#pool.map(save, stocklist)意思就是从stocklist中取每一个元素送入save的函数中运行。最后把上段代码的errorlist打印成文件

pool = ThreadPool(8)
try:
    pool.map(save, stocklist.values)
except:
    #pool.map(save, stocklist)
    print("pool.map() except!"+ str(stocklist.shape))
    
fdf = pd.DataFrame(errorlist)
fdf.to_csv("Notsaved.txt")

pool.close()
pool.join()
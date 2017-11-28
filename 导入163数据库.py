# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd #注2
import os

repfiles = os.listdir("163rep")#得到文件夹下的所有文件名称
errorlist = []

conn=sqlite3.connect('163history.db') #注4 163history.db是存放股票历史DB，非线程安全
#engine = create_engine('sqlite:///163history.db', echo = False) #注5线程安全的
query = "select name from sqlite_master where type='table' order by name" #注10 意思是获取所有表名
alreadylist = pd.read_sql(query, conn) #注11 另一种读数据库的方法，直接用pandas读取数据库，alreadylist是DataFrame类型，有代码的那一列名为name

print(alreadylist )
print("-------------------------")
def imDB(file):
    name = file[:-5]
    print(name)
    if name not in alreadylist.values :
        try:
            ff = pd.read_excel ("163rep/" + file, index_col= 0) #, encoding= "GBK")
            if "transnum" not in ff.columns:
                ff["transnum"] = 0
                print(file + "has no transnum---------------")
            #date列作为了索引，重新列名赋值去掉索引
            ff.columns = ["code", "name", "close", "high", "low", "open", "rclose", 
                          "amountch", "quotech", "turnover", "volume", "amount", "total", "circulation", "transnum"] #原来规整数据一部分索引有问题
            ff = ff.sort_index(ascending=True)        
            ff.to_sql(name, conn)
        except :
            print(file)
            errorlist .append (file)

for ef in repfiles :
    imDB(ef)

#扫尾工作
pd.DataFrame (errorlist).to_excel ("errorlist.xlsx")
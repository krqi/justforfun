# coding=utf-8
import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import json 
import sqlite3 
from bs4 import BeautifulSoup 
from  pandas import Series ,DataFrame
from urllib.request import urlopen, Request
from sqlalchemy import create_engine #注1 sqlalchemy是Python自带的与数据库联结的包，导入创建数据库联结的函数
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

errlist = []
pd.set_option ("display.width", 2000)
now = dt.datetime.now()#  ->这是时间数组格式
today = now.strftime("%Y-%m-%d") #转换为指定的格式:
lnow = now - dt.timedelta(days=1) #用今天日期减掉时间差，参数为1天，获得昨天的日期
yesterday = lnow.strftime("%Y-%m-%d") #转换为指定的格式:
#保存详情数据DB
engine = create_engine('sqlite:///save/'+str(today)+'lhbdetail.db', echo = False) #注5 创建与sqlite数据库的联结，名字为History.db

pdf = pd.DataFrame()

#龙虎榜列表
#dzbaseurl = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=5000,page=1,sortRule=-1,sortType=,startDate=2017-06-15,endDate=2017-07-06,gpfw=0,js=var%20data_tab_2.html"
dzbaseurl = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=1000,page=1,sortRule=-1,sortType=,startDate="+str(yesterday)+",endDate="+str(today)+",gpfw=0,js=var%20data_tab_2.html"

dzurl = dzbaseurl# + str(x)
rsp = urlopen(dzurl).read()
js = json.loads(rsp.decode('gb2312')[15:])#滤掉前面这段var data_tab_2=
pp = pd.DataFrame (list(js['data']))#只取有用数据
pp.columns= ['买入额', '涨跌幅', '收盘价', '上榜原因', '解读', '换手率', '解读', 'JGBMoney', 
             'JGBSumCount', 'JGJMMoney', 'JGSMoney', 'JGSSumCount', '净买额', '净买额占总成交比', 'Ltsz', 
             'Ntransac', 'Oldid', 'Rchange10dc', 'Rchange10do', 'Rchange15dc', 'Rchange15do', 'Rchange1dc', 
             'Rchange1do', 'Rchange1m', 'Rchange1y', 'Rchange20dc', 'Rchange20do', 'Rchange2dc', 'Rchange2do',
             'Rchange30dc', 'Rchange30do', 'Rchange3dc', 'Rchange3do', 'Rchange3m', 'Rchange5dc', 'Rchange5do', 
             'Rchange6m', '代码', '名称', '卖出额', 'SumCount', '日期', '市场总成交额', '龙虎榜成交额', '成交额占总成交比']
pp = pp[['日期', '代码', '名称', '买入额', '涨跌幅', '收盘价', '上榜原因', '解读', '换手率', '净买额', '净买额占总成交比', '卖出额', 
         '市场总成交额', '龙虎榜成交额', '成交额占总成交比', 'Ltsz', 'Ntransac', 'Oldid', 'Rchange1m', 'Rchange1y', 'Rchange3m', 'Rchange6m']]
pdf = pdf.append (pp)
pdf.to_excel (str(today)+'龙虎榜列表.xlsx')
#东财龙虎榜详情
#lhblistbaseurl = 'http://data.eastmoney.com/stock/lhb,2017-05-19,000062.html'
lhblistbaseurl1 = 'http://data.eastmoney.com/stock/lhb,'
lhblistbaseurl2 = ','

lhblistbaseurl3 = '.html'
#today = dt.date.today()

lday = '9999-01-30' #上次日期，便于比较不重复
lcode = '888888'

def down_dc_lhb_detail(row):
    global lday,lcode #声明该变量是全局变量
    totalrow = pdf[:10].copy() #只是为了初始化10条数据结构使用，数据并不使用
    for c in range(0, len(totalrow.columns)):
        totalrow.iloc[:,c] = row[c] #初始化该数据结构每列值为传入的行值

    day = str(row[0])#str(row['日期'])
    code = str(row[1])#str(row['代码'])
    code = code .zfill (6) #股票代码自动前面补0
    if day == lday and code == lcode:
        return  
    lday = day
    lcode = code 
    
    totalrow ["代码"] = code
    print(day + ':' + code)
    lhblisturl = lhblistbaseurl1 + day + lhblistbaseurl2 + code + lhblistbaseurl3 
    try:     
        try:
            rsp = urlopen(lhblisturl).read()
        except :
            rsp = urlopen(lhblisturl).read()
        rsp = rsp.decode ('gb2312')
        soup = BeautifulSoup (rsp)
        #买入TOP5
        d2 = soup.find(class_="default_tab stock-detail-tab")
        d2 = d2.find('tbody')
        top5 = []
        for r in d2.findAll("tr"):
            cells = r.findAll("td")
            onerow = []
            for c in cells :
                cc = c.find(class_="sc-name")
                if cc is not None:
                    cc = cc.findNext()
                    onerow .append (cc.findNext(text=True))
                onerow .append (c.find(text=True))
            top5 .append (onerow )

        #卖出TOP5
        d2 = soup.find(class_="default_tab tab-2")
        d2 = d2.find('tbody')

        for r in d2.findAll("tr"):
            cells = r.findAll("td")
            onerow = []
            for c in cells :
                cc = c.find(class_="sc-name")
                if cc is not None:
                    cc = cc.findNext()
                    onerow .append (cc.findNext(text=True))
                onerow .append (c.find(text=True))
            top5 .append (onerow ) 
            
        pp = pd.DataFrame (top5)#只取有用数据
        #整理列名
        pp.columns= ['买卖排名','交易营业部名称','2','买入金额(万)','买入占总成交比例','卖出金额(万)','卖出占总成交比例','净额(万)']
        pp = pp[['买卖排名','交易营业部名称','买入金额(万)','买入占总成交比例','卖出金额(万)','卖出占总成交比例','净额(万)']]
        pp = pp.iloc [:-1,]
        
        allone = pd.concat ([totalrow ,pp],axis= 1)
        allone.to_sql("lhbdetail", engine, if_exists='append') #注6 写入数据库，if_exists='append'意为追加的形式。
    except :
        print("ERR:" ,day,":", code)
        errlist .append (day)

#多线程处理
pool = ThreadPool(1)
try:
    pool.map(down_dc_lhb_detail, pdf.values)
except:
    print("pool.map() except!"+ str(pdf.shape))
    
pool.close()
pool.join()

pd.DataFrame (errlist ).to_excel ('errlst.xlsx')
pf = pd.DataFrame()
pf = pd.read_sql_table('lhbdetail', engine)
pf = pf.sort_values(['日期','代码','index'])
#pf17 = pdf[pdf.日期 > '2017-01-01']
#pf16 = pdf[pdf.日期 < '2017-01-01']
pf.to_excel(str(today)+'龙虎榜详情列表.xlsx')
#pf16.to_excel('F:/krqi/深度学习/证券/zcl/20160415龙虎榜详情列表.xlsx')

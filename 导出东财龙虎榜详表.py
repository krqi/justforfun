# coding=utf-8
import pandas as pd
import datetime as dt
import tushare as ts
from sqlalchemy import create_engine #注1 sqlalchemy是Python自带的与数据库联结的包，导入创建数据库联结的函数
from  pandas import Series ,DataFrame

#pd.set_option ("display.width", 2000)
#pdf = pd.DataFrame()

#pdf = pd.DataFrame()

##龙虎榜列表
#engine = create_engine('sqlite:///F:/krqi/深度学习/证券/zcl/save/20170520lhbdetail.db', echo = False) #注5 创建与sqlite数据库的联结，名字为History.db

#pdf = pd.read_sql_table('lhbdetail', engine)
#pdf = pdf.sort_values('日期')
#pf17 = pdf[pdf.日期 > '2017-01-01']
#pf16 = pdf[pdf.日期 < '2017-01-01']
#pf17.to_excel('F:/krqi/深度学习/证券/zcl/2017龙虎榜详情列表.xlsx')
#pf16.to_excel('F:/krqi/深度学习/证券/zcl/20160415龙虎榜详情列表.xlsx')

pf17 = pd.read_excel('F:/krqi/深度学习/证券/zcl/2017龙虎榜详情列表排序.xlsx')
pf17 = pf17.sort_values(['日期','代码','index'])
pf17.to_excel('F:/krqi/深度学习/证券/zcl/2017龙虎榜详情列表规整.xlsx')

pf16 = pd.read_excel('F:/krqi/深度学习/证券/zcl/20160415龙虎榜详情列表排序.xlsx')
pf16 = pf16.sort_values(['日期','代码','index'])
pf16.to_excel('F:/krqi/深度学习/证券/zcl/20160415龙虎榜详情列表规整.xlsx')

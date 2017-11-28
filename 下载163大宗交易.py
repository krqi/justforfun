# coding=utf-8
import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import json 
from bs4 import BeautifulSoup 
from  pandas import Series ,DataFrame
from urllib.request import urlopen, Request

pd.set_option ("display.width", 2000)
pdf = pd.DataFrame()
##163大宗交易地址，page=0是分页，count=1000是每次查询数量
##dzurl = "http://quotes.money.163.com/hs/marketdata/service/dzjy.php?host=/hs/marketdata/service/dzjy.php&page=0&sort=PUBLISHDATE&order=desc&count=2000&type=query"#"http://data.eastmoney.com/stock/lhb/yyb/80035422.html"
dzbaseurl = "http://quotes.money.163.com/hs/marketdata/service/dzjy.php?host=/hs/marketdata/service/dzjy.php&sort=PUBLISHDATE&order=desc&type=query&count=1000&page="
for x in range(0,10):
    dzurl = dzbaseurl + str(x)
    rsp = urlopen(dzurl).read()
    js = json.loads(rsp.decode('utf-8'))
    pp = pd.DataFrame (list(js['list']))#只取有用数据
    pp.columns= ['CODE','卖方','成交量（万股）','成交价','折价率（百分比）','成交金额（万）','买方','深市沪市','NAME','NO','日期','名称','STYPE','代码','当天收盘价']
    pp = pp[['日期','代码','名称','卖方','成交量（万股）','成交价','成交金额（万）','买方','折价率（百分比）','当天收盘价']]#,'CODE','深市沪市','NAME','NO','STYPE']]
    pdf = pdf.append (pp)
pdf.to_excel ("down/163大宗交易列表.xlsx")


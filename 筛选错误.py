# coding=utf-8
#import tushare as ts
#import sqlite3
import pandas as pd #注2
#import json 
#from urllib.request import urlopen, Request
#from sqlalchemy import create_engine
#from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

pd.set_option ("display.width", 2000)
pdf = pd.DataFrame()
#engine = create_engine('sqlite:///err.db', echo = False) #注5
pf1 = pd.DataFrame ()
pf1 = pd.read_excel('save/东财龙虎榜列表2017-05-19.xlsx')
pf2 = pd.read_excel('save/errorlist.xlsx')

pf3 = pf1[:507]

pf2['t'] = pf2['代码']
for ii in range(0,len(pf2.index)):
    cc = pf2.iloc[ii,1]
    #pf2.iloc[ii,1] = str(cc).zfill(6)
    pf2.iloc[ii,2] = str(pf2.iloc[ii,0])[:10] + str(pf2.iloc[ii,1])

print(pf2[:3])

n = 0
for i,row in pf1.iterrows() :
    if n >= len(pf3.index ):
        break 
    x = str(row['日期']) + str(row['代码'])
    if x in pf2['t'].values:
        for c in range(0, len(pf1.columns)):
            pf3.iloc[n,c] = row[c]        
    n = n + 1
    
pf3.to_excel('save/errmatch.xlsx')

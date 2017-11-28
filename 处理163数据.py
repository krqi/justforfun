# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd #注2
import os
from urllib.request import urlopen, Request
from multiprocessing.dummy import Pool as ThreadPool #注3 多线程

files= os.listdir("163") #得到文件夹下的所有文件名称  
repfile = os.listdir("163rep")
errorlist = []
for file in files: #遍历文件夹
    outname = str(file[:-3])+ ".xlsx"
    if outname in repfile :
        continue 
    print(file)
    try:
        ff = pd.read_csv ("163/" + file, encoding= "GBK")
        ff.columns = ["date", "code", "name", "close", "high", "low", "open", "rclose", "amountch", "quotech", "turnover", "volume", "amount", "total", "circulation", "transnum"]
    except :
        ff.columns = ["date", "code", "name", "close", "high", "low", "open", "rclose", "amountch", "quotech", "turnover", "volume", "amount", "total", "circulation"]
        errorlist .append (file)
    ff = ff.set_index ("date")
    ff = ff[ff["close"]>0]

    df = pd.DataFrame (ff)
    
    #复权处理
    weight = 1.0
    for i in range(1,len(ff.index)) :
        if ff["rclose"][i - 1] < ff["close"][i]:
            weight = ff["rclose"][i - 1]/ff["close"][i] #weight * ff["rclose"][i - 1]/ff["close"][i]
            df.iloc[i:,2:6] = ff.iloc[i:,2:6]*weight
            df["volume"][i:] = ff["volume"][i:]/weight
    df.iloc[:,2:6] = df.iloc[:,2:6].round(2)
    df["volume"] = df["volume"].round(0)
    df.to_excel ("163rep/" + str(file[:-3])+ ".xlsx")
#准备工作
pd.DataFrame (errorlist).to_excel ("errorlist.xlsx")
import tushare as ts
import sqlite3
import pandas as pd #注2
import datetime as dt

now = dt.datetime.now()#  ->这是时间数组格式
today = now.strftime("%Y%m%d") #转换为指定的格式:
nextyear = int(now.strftime ("%Y")) + 1

#准备工作
conn1=sqlite3.connect('基本面/' + str(today) + 'StocklistBase.db') #存放股票列表的数据库

#获取股票列表并插入列表库
stocklist = ts.get_stock_basics()
stocklist.to_sql("Allist", conn1, if_exists="replace")

err = []
for y in range(2010, nextyear): #年份
    """   
    #年报数据还没有，先注销掉
    print(y)
    #获取2014年的业绩报表数据
    try:
        rp = ts.get_report_data(y)
        rp["year"] = y
        rp.to_sql("YReport", conn1, if_exists="replace")
    except :
        err.append(str(y) + " report")
        print(err[-1])
        
    #获取盈利能力数据
    try:
        pf = ts.get_profit_data(y)
        pf["year"] = y
        pf.to_sql("YProfit", conn1, if_exists="replace")
    except :
        err.append(str(y) + " profit")
        print(err[-1])
        
    #获取营运能力数据
    try:
        op = ts.get_operation_data(y)
        op["year"] = y
        op.to_sql("YOperation", conn1, if_exists="replace")
    except :
        err.append(str(y) + " operation")
        print(err[-1])
        
    #获取成长能力数据
    try:
        gr = ts.get_growth_data(y)
        gr["year"] = y
        gr.to_sql("YGrowth", conn1, if_exists="replace")
    except :
        err.append(str(y) + " growth")
        print(err[-1])
        
    #获取偿债能力数据
    try:
        de = ts.get_debtpaying_data(y)
        de["year"] = y
        de.to_sql("YDebtpaying", conn1, if_exists="replace")
    except :
        err.append(str(y) + " debtpaying")
        print(err[-1])
        
    #获取现金流数据
    try:
        ca = ts.get_cashflow_data(y)
        ca["year"] = y
        ca.to_sql("YCashflow", conn1, if_exists="replace")
    except :
        err.append(str(y) + " cashflow")
        print(err[-1])
    """

    #季度报
    for q in range(1,5): #d季度
        yearq = str(y)+"-" + str(q)
        print(yearq)
        #获取2014年第3季度的业绩报表数据
        try:
            rp = ts.get_report_data(y,q)
            rp["year"] = y
            rp["Q"] = q
            rp.to_sql("QReport", conn1, if_exists="replace")
        except :
            err.append(yearq + " report")
            print(err[-1])
            
        #获取盈利能力数据
        try:
            pf = ts.get_profit_data(y,q)
            pf["year"] = y
            pf["Q"] = q
            pf.to_sql("QProfit", conn1, if_exists="replace")
        except :
            err.append(yearq + " profit")
            print(err[-1])
            
        #获取营运能力数据
        try:
            op = ts.get_operation_data(y,q)
            op["year"] = y
            op["Q"] = q
            op.to_sql("QOperation", conn1, if_exists="replace")
        except :
            err.append(yearq + " operation")
            print(err[-1])
            
        #获取成长能力数据
        try:
            gr = ts.get_growth_data(y,q)
            gr["year"] = y
            gr["Q"] = q
            gr.to_sql("QGrowth", conn1, if_exists="replace")
        except :
            err.append(yearq + " growth")
            print(err[-1])
            
        #获取偿债能力数据
        try:
            de = ts.get_debtpaying_data(y,q)
            de["year"] = y
            de["Q"] = q
            de.to_sql("QDebtpaying", conn1, if_exists="replace")
        except :
            err.append(yearq + " debtpaying")
            print(err[-1])
            
        #获取现金流数据
        try:
            ca = ts.get_cashflow_data(y,q)
            ca["year"] = y
            ca["Q"] = q
            ca.to_sql("QCashflow", conn1, if_exists="replace")
        except :
            err.append(yearq + " cashflow")
            print(err[-1])
                
pd.DataFrame(err).to_excel("err.xls")
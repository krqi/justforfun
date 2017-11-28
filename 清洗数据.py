import pandas as pd
import sqlite3
from multiprocessing.dummy import Pool as ThreadPool

#清洗数据库
#注1：这句SQL语句的意思是以date分组，删除重复的行
#注2：最后执行cur.execute(...)完后要con.commit()提交，才能有效

con = sqlite3.connect('History.db')
query1 = "select name from sqlite_master where type='table' order by name"
stocklist = pd.read_sql(query1, con).name

delstock = []
f = open('Deleted.txt', 'w')
for stock in stocklist:
    query2 = "select * from '%s' order by date" %stock
    df = pd.read_sql(query2, con)
    cur=con.cursor()
    query4 = "delete from '%s' where rowid not in(select max(rowid) from '%s' group by date)" %(stock, stock) #注1
    cur.execute(query4)
    con.commit()

con.close()
print >> f, delstock
f.close()

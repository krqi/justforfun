import pandas as pd
import numpy as np
from  pandas import Series ,DataFrame
#----------------------------------------------------------------------
ff = pd.read_csv ("000651.csv")
print(ff.shape)
print(ff.iloc[:2])
ff.columns = ["date", "code", "name", "close", "high", "close", "open", "rclose", 
              "amountch", "quotech", "turnover", "volume?", "amount?", "total", "circulation ", "transnum"]
print(ff.iloc[:2])
print("----------------")

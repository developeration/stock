# coding:utf8

from numpy import disp
from pandas import DataFrame
from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os
import pandas as pd
from pyspark.sql.types import  *
from sklearn import datasets,linear_model
from IPython.display import display

if __name__ == "__main__": 

    spark = SparkSession.builder \
                .appName("ml_analyze_daily") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', 'file:///work/dev/stock/settings/xsetting.py') \
                .getOrCreate()
    sc = spark.sparkContext 
    stock_basic_path = xsetting.stock_basic_path()
    daily_path = xsetting.daily_path()
    analyze_daily_path = xsetting.analyze_daily_path()

    files_list = os.listdir("/work/dev/stock/datasource/daily/")
    df = pd.DataFrame(files_list)
    
    def analyzesplit(filename):
        filepath = daily_path+filename
        print(filepath)
        rf = spark.read.format("json").load(filepath)
        df = rf.toPandas().sort_values(by=['trade_date'])
        dfcount = df.shape[0]
        data_return_data = list()
        try: 
             stock_x_dic = {
                     'open':df["open"].values,
                     'high':df["high"].values,
                     'low':df["low"].values,
                     'close':df["close"].values,
                     'amount':df["amount"].values
             }
             #print(stock_x_dic)
             stock_x = pd.DataFrame(stock_x_dic).values 
             stock_x_v = stock_x[0:len(stock_x)-1]
             stock_x_current = stock_x[len(stock_x)-1:]
             stock_y_v = df["close"].tolist()
             del stock_y_v[0]
             #stock_y_v = stock_y[1,len(stock_y)]
             
             #print(len(stock_x_v))
             #print(len(stock_y_v))
        #      display(stock_y_v)
        #      print(len(stock_x_v)) 
        #      print(len(stock_y_v))  
             #print(stock_x_current)
             model = linear_model.LinearRegression()
             model.fit(stock_x_v,stock_y_v)
             print(stock_y_v[len(stock_y_v)-1:])
             print(model.predict(stock_x_current))
        except Exception as e:
            print(e)

        return "~"
    print("东风汽车 600006")
    analyzesplit("600006.SH")
#     print("国发股份 600538")
#     analyzesplit("600538.SH")
#     print("山鹰国际 600567")
#     analyzesplit("600567.SH")
#     print("永鼎股份 600105")
#     analyzesplit("600105.SH")
#     print("烽火通讯 600498")
#     analyzesplit("600498.SH")
#     print("招商证券 600999")
#     analyzesplit("600999.SH")
#     print("恒瑞医药 600276")
#     analyzesplit("600276.SH")
    # print("平安银行 000001")
    # analyzesplit("000001.SZ")
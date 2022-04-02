# coding:utf8



from pandas import DataFrame
from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os
import pandas as pd

if __name__ == "__main__": 

    spark = SparkSession.builder \
                .appName("ml_analyze_daily") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', 'file:///work/dev/stock/settings/xsetting.py') \
                .getOrCreate()
    sc = spark.sparkContext
    pro = ts.pro_api(xsetting.tosharekey())

    stock_basic_path = xsetting.stock_basic_path()
    daily_path = xsetting.daily_path()
    analyze_daily_path = xsetting.analyze_daily_path()

    data_list_path = xsetting.stock_basic_path()+'SH'
    print(data_list_path)
    data_list = spark.read.format("json").load(data_list_path)
    listdata =  pd.DataFrame()
    def analyzedailydata(item): 
        #if item.ts_code=='600105.SH' or item.ts_code=='600999.SH':
            item_daily_path = daily_path+item.ts_code
            data_daily = spark.read.format("json").load(item_daily_path)
            #print(data_daily.count())
            #data_daily.mapInPandas()
            return data_daily.toPandas()

    for item in data_list.collect():
        x = analyzedailydata(item)
        listdata=pd.concat([listdata,x])
    
    values = listdata.values.tolist()
    columns = listdata.columns.tolist() 
    data_spark = spark.createDataFrame(values, columns)

    data_spark.show()
    
    

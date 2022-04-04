# coding:utf8

from pandas import DataFrame
from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os
import pandas as pd
from pyspark.sql.types import  *

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
            for index in range(0,dfcount-10): 
                data_return_data_item = list()
                data_return_data_item.append(df.iloc[index].ts_code) 
                data_return_data_item.append(df.iloc[index].trade_date) 
                data_return_data_item.append(float(df.iloc[index].change))
                data_return_data_item.append(float(df.iloc[index+1].change))
                data_return_data_item.append(float(df.iloc[index+2].change))
                data_return_data_item.append(float(df.iloc[index+3].change))
                data_return_data_item.append(float(df.iloc[index+4].change))
                data_return_data_item.append(float(df.iloc[index+5].change))
                data_return_data_item.append(float(df.iloc[index+6].change))
                data_return_data_item.append(float(df.iloc[index+7].change))
                data_return_data_item.append(float(df.iloc[index+8].change))
                data_return_data_item.append(float(df.iloc[index+9].change))
                data_return_data.append(data_return_data_item)
        except Exception as e:
            print(e)
        schema = StructType(
            [
                StructField("ts_code", StringType(), True),
                StructField("trade_date", StringType(), True),
                StructField("c1", FloatType(), True),
                StructField("c2", FloatType(), True),
                StructField("c3", FloatType(), True),
                StructField("c4", FloatType(), True),
                StructField("c5", FloatType(), True),
                StructField("x1", FloatType(), True),
                StructField("x2", FloatType(), True),
                StructField("x3", FloatType(), True),
                StructField("x4", FloatType(), True),
                StructField("x5", FloatType(), True),
            ]
        ) 
        data_spark = spark.createDataFrame(data_return_data, schema)
        savepath = analyze_daily_path+filename
        data_spark.write.mode("overwrite").format("json").save(savepath)
        
        return "~"
    x = df.applymap(analyzesplit)
    #print(x)
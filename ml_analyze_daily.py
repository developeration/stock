# coding:utf8

from pandas import DataFrame
from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os
import pandas as pd
from pyspark.sql.types import  *
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
            for index in range(0,dfcount-10): 
                data_return_data_item = list()
                data_return_data_item.append(df.iloc[index].ts_code) 
                data_return_data_item.append(df.iloc[index].trade_date) 
                pct_chg1 = float(df.iloc[index].pct_chg)
                pct_chg2 = float(df.iloc[index+1].pct_chg)
                pct_chg3 = float(df.iloc[index+2].pct_chg)
                pct_chg4 = float(df.iloc[index+3].pct_chg)
                pct_chg5 = float(df.iloc[index+4].pct_chg)
                data_return_data_item.append(pct_chg1)
                data_return_data_item.append(pct_chg2)
                data_return_data_item.append(pct_chg3)
                data_return_data_item.append(pct_chg4)
                data_return_data_item.append(pct_chg5)

                
                data_return_data_item.append(1.0*(1+pct_chg1)*(1+pct_chg2)*(1+pct_chg3)*(1+pct_chg4)*(1+pct_chg5)-1.0)

                pct_chg6 = float(df.iloc[index+5].pct_chg)
                pct_chg7 = float(df.iloc[index+5].pct_chg)
                pct_chg8 = float(df.iloc[index+5].pct_chg)
                pct_chg9 = float(df.iloc[index+5].pct_chg)
                pct_chg10 = float(df.iloc[index+5].pct_chg)

                data_return_data_item.append(pct_chg6)
                data_return_data_item.append(pct_chg7)
                data_return_data_item.append(pct_chg8)
                data_return_data_item.append(pct_chg9)
                data_return_data_item.append(pct_chg10)

                data_return_data_item.append(1.0*(1+pct_chg6)*(1+pct_chg7)*(1+pct_chg8)*(1+pct_chg9)*(1+pct_chg10)-1.0)

                data_return_data.append(data_return_data_item)
        
            schema = StructType(
                [
                    StructField("ts_code", StringType(), True),
                    StructField("trade_date", StringType(), True),
                    StructField("c1", FloatType(), True),
                    StructField("c2", FloatType(), True),
                    StructField("c3", FloatType(), True),
                    StructField("c4", FloatType(), True),
                    StructField("c5", FloatType(), True),
                    StructField("ct", FloatType(), True),
                    StructField("x1", FloatType(), True),
                    StructField("x2", FloatType(), True),
                    StructField("x3", FloatType(), True),
                    StructField("x4", FloatType(), True),
                    StructField("x5", FloatType(), True),
                    StructField("xt", FloatType(), True),
                ]
            ) 
            data_spark = spark.createDataFrame(data_return_data, schema)
            savepath = analyze_daily_path+filename
            data_spark.write.mode("overwrite").format("json").save(savepath)
            
        except Exception as e:
            print(e)

        return "~"
    x = df.applymap(analyzesplit)
    #print(x)
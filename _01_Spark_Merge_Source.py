from pyspark import SparkConf
from _Setting import StockSetting
import tushare  as ts
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

#spark-submit  --master yarn --py-files ./_Setting.py --deploy-mode cluster --conf "spark.default.parallelism=15" ./_Spark_makeSource_Daily.py
#nohup spark-submit  --master yarn --py-files ./_Setting.py --deploy-mode cluster ./_Spark_makeSource_Daily.py > spark.log &

if __name__ == "__main__":
    settings = StockSetting()
    spark = SparkSession.builder \
        .appName("_01_Spark_Merge_Source") \
        .master("yarn") \
        .config('spark.submit.pyFiles', '/work/dev/stock/_Setting.py') \
        .getOrCreate() 
        # .appName("_01_Spark_Merge_Source") \
        # .getOrCreate()  

    pro = ts.pro_api(settings.tushareKey)
    conf = SparkConf()
    conf.set("spark.default.parallelism","15")
          
    def getdailydata(item):
        dailypath = settings.datasource_daily_path+item.ts_code
        daily_data = spark.read.format("json").load(dailypath)
        return daily_data

    def getdaily_basicdata(item):
        dailypath = settings.datasource_daily_basic_path+item.ts_code
        daily_data = spark.read.format("json").load(dailypath)
        return daily_data

    def getmoneyflowdata(item):
        dailypath = settings.datasource_moneyflow_path+item.ts_code
        daily_data = spark.read.format("json").load(dailypath)
        return daily_data

    savepath = settings.datasource_path+"stock_basic_main"
    data_base_main = spark.read.format("json").load(savepath)


    daily_data = None
    daily_basic_data = None
    daily_moneyflow_data = None
    stock_basic_main_list = data_base_main.collect()
    index = 0
    for item in stock_basic_main_list:
        index = index+1
        if(index % 50 == 49 ):
            print(index)
        try:
            return_value = getdailydata(item)
            if(daily_data == None):
                daily_data = return_value
            else :
                daily_data = daily_data.unionAll(return_value)

            
            return_value = getdaily_basicdata(item)
            if('dv_ratio' in return_value.columns):
                return_value = return_value.drop('dv_ratio')
            if('dv_ttm' in return_value.columns):
                return_value = return_value.drop('dv_ttm')
            if(daily_basic_data == None): 
                daily_basic_data = return_value
            else :
                daily_basic_data = daily_basic_data.unionAll(return_value)

            
            return_value = getmoneyflowdata(item)
            if(daily_moneyflow_data == None):
                daily_moneyflow_data = return_value
            else :
                daily_moneyflow_data = daily_moneyflow_data.unionAll(return_value)
        except Exception as ex:
            print(ex)


    savepath = settings.data_all_path+"daily_data_ALL"
    daily_data.write.mode("overwrite").format("json").save(savepath)
    savepath = settings.data_all_path+"daily_basic_data_ALL"
    daily_basic_data.write.mode("overwrite").format("json").save(savepath)
    savepath = settings.data_all_path+"daily_moneyflow_data_ALL"
    daily_moneyflow_data.write.mode("overwrite").format("json").save(savepath)

    print("Finished")
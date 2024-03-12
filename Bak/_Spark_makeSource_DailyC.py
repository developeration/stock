from pyspark import SparkConf
from _Setting import StockSetting
import tushare  as ts
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

#spark-submit  --master yarn --py-files ./_Setting.py --deploy-mode cluster  ./_Spark_makeSource_DailyC.py 

if __name__ == "__main__":
    settings = StockSetting()
    spark = SparkSession.builder \
        .appName("_Spark_makeSource") \
        .master("yarn") \
        .config('spark.submit.pyFiles', '/work/dev/stock/_Setting.py') \
        .getOrCreate() 
        # .appName("_Spark_makeSource_DailyC") \
        # .getOrCreate() 

    # conf = SparkConf()
    # conf.set("spark.default.parallelism","15")
    sc = spark.sparkContext
    pro = ts.pro_api(settings.tushareKey)


    # data_local = pro.stock_basic(exchange='', list_status='L',market='主板')
    # data_hadop = spark.createDataFrame(data_local)
    savepath = settings.datasource_path+"stock_basic_main"
    # data_hadop.write.mode("overwrite").format("json").save(savepath)

    data_hadop = spark.read.format("json").load(savepath)
    #Debug
    #data_hadop.show()

    def getdailydata(item):
        try:
            savepath = settings.datasource_moneyflow_path+item.ts_code
            if(settings.file_exists(sc,savepath) == False):
                print("moneyflow",item.ts_code)
                data_daily_local = pro.moneyflow(ts_code=item.ts_code )
                if data_daily_local.empty :
                    return
                data_daily_hadop = spark.createDataFrame(data_daily_local)
                
                data_daily_hadop.write.mode("overwrite").format("json").save(savepath)
        except Exception as e:
            print(item.ts_code,"moneyflow",e)
    #data_hadop.foreach(getdailydata)
    stock_list = data_hadop.collect()
    for item in stock_list:
        getdailydata(item)
        
    print("Finished")
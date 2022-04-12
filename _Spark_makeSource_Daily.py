from _Setting import StockSetting
import tushare  as ts
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

#spark-submit  --master yarn --py-files ./_Setting.py --deploy-mode cluster ./_Spark_makeSource_Daily.py
#nohup spark-submit  --master yarn --py-files ./_Setting.py --deploy-mode cluster ./_Spark_makeSource_Daily.py > spark.log &

if __name__ == "__main__":
    settings = StockSetting()
    spark = SparkSession.builder \
        .appName("_Spark_makeSource") \
        .master("local[*]") \
        .config('spark.submit.pyFiles', '/work/dev/stock/_Setting.py') \
        .getOrCreate() 
        # .appName("_Spark_makeSource") \
        # .getOrCreate() 

    pro = ts.pro_api(settings.tushareKey)

    data_local = pro.stock_basic(exchange='', list_status='L',market='主板')
    data_hadop = spark.createDataFrame(data_local)
    savepath = settings.datasource_path+"stock_basic_main"
    data_hadop.write.mode("overwrite").format("json").save(savepath)

    data_hadop = spark.read.format("json").load(savepath)
    #Debug
    data_hadop.show()

    def getdailydata(item):
        try:
            data_daily_local = pro.daily(  ts_code=item.ts_code )
            if data_daily_local.empty :
                return
            data_daily_hadop = spark.createDataFrame(data_daily_local)
            savepath = settings.datasource_daily_path+item.ts_code
            data_daily_hadop.write.mode("overwrite").format("json").save(savepath)
        except Exception as e:
            print(item.ts_code,"daily",e)
        
        try:
            data_daily_local = pro.daily_basic(  ts_code=item.ts_code )
            if data_daily_local.empty :
                return
            schema = StructType([
                StructField("ts_code", StringType(), True)
                , StructField("trade_date", StringType(), True)
                , StructField("close", DoubleType(), True)
                , StructField("turnover_rate", DoubleType(), True)
                , StructField("turnover_rate_f", DoubleType(), True)
                , StructField("volume_ratio", DoubleType(), True)
                , StructField("pe", DoubleType(), True)
                , StructField("pe_ttm", DoubleType(), True)
                , StructField("pb", DoubleType(), True)
                , StructField("ps", DoubleType(), True)
                , StructField("ps_ttm", DoubleType(), True)
                , StructField("dv_ratio", DoubleType(), True)
                , StructField("dv_ttm", DoubleType(), True)
                , StructField("total_share", DoubleType(), True)
                , StructField("float_share", DoubleType(), True)
                , StructField("free_share", DoubleType(), True)
                , StructField("total_mv", DoubleType(), True)
                , StructField("circ_mv", DoubleType(), True)
                ])
            #data_daily_hadop = spark.createDataFrame(data_daily_local)
            values = data_daily_local.values.tolist()
            #columns = data_daily_local.columns.tolist() 
            data_daily_hadop = spark.createDataFrame(values, schema)
            savepath = settings.datasource_daily_basic_path+item.ts_code
            data_daily_hadop.write.mode("overwrite").format("json").save(savepath)
        except Exception as e:
            print(item.ts_code,"daily_basic",e)

        try:
            data_daily_local = pro.moneyflow(ts_code=item.ts_code )
            if data_daily_local.empty :
                return
            data_daily_hadop = spark.createDataFrame(data_daily_local)
            savepath = settings.datasource_moneyflow_path+item.ts_code
            data_daily_hadop.write.mode("overwrite").format("json").save(savepath)
        except Exception as e:
            print(item.ts_code,"moneyflow",e)
    #data_hadop.foreach(getdailydata)
    for item in data_hadop.collect():
        getdailydata(item)
        
    print("Finished")
# coding:utf8



from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os

if __name__ == "__main__": 

    spark = SparkSession.builder \
                .appName("ml_datasource_daily") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', '/work/dev/settings/xsetting.py') \
                .getOrCreate()
    sc = spark.sparkContext
    pro = ts.pro_api(xsetting.tosharekey())

    stock_basic_path = xsetting.stock_basic_path()
    daily_path = xsetting.daily_path()

    data_list_path = xsetting.stock_basic_path()+'SH'
    data_list = spark.read.format("json").load(data_list_path)
    
    def getdailydata(item): 
        if item.ts_code=='600105.SH' or item.ts_code=='600999.SH':
            #xsetting.deletefile(sc,daily_path+item.ts_code)
            print(item.ts_code)
            for _year in range(2010,2022):
                data_list = pro.daily(
                        ts_code=item.ts_code,
                        start_date=str(_year)+'0101',
                        end_date=str(_year)+'1231'
                    )
                values = data_list.values.tolist()
                columns = data_list.columns.tolist() 
                
                data_spark = spark.createDataFrame(values, columns)
                savepath = daily_path+item.ts_code
                data_spark.write.mode("append").format("json").save(savepath)
    for item in data_list.collect():
        getdailydata(item)
    
    

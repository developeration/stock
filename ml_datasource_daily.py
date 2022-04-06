# coding:utf8



from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
import os
import time
if __name__ == "__main__": 

    spark = SparkSession.builder \
                .appName("ml_datasource_daily") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', 'file:///work/dev/stock/settings/xsetting.py') \
                .getOrCreate()
    sc = spark.sparkContext
    pro = ts.pro_api(xsetting.tosharekey())

    stock_basic_path = xsetting.stock_basic_path()
    daily_path = xsetting.daily_path()

    data_list_path = xsetting.stock_basic_path()+'MAIN'
    data_list = spark.read.format("json").load(data_list_path)
    
    def getdailydata(item):  
        #if (item.ts_code=='600105.SH' or item.ts_code=='600999.SH') or True:
            #xsetting.deletefile(sc,daily_path+item.ts_code)
            # start_year = int(item.list_date[0:4])+1
            # for _year in range(start_year,2023):
                try:
                    print(item.ts_code)
                    _year = 2000
                    data_list = pro.daily(
                            ts_code=item.ts_code,
                            start_date=str(_year)+'0101',
                            end_date='20221231'
                        )
                    if data_list.empty :
                        return
                    values = data_list.values.tolist()
                    columns = data_list.columns.tolist() 
                    
                    data_spark = spark.createDataFrame(values, columns)
                    savepath = daily_path+item.ts_code
                    data_spark.write.mode("overwrite").format("json").save(savepath)
                except Exception as e:
                    print(e)
    for item in data_list.collect():
        getdailydata(item)
        time.sleep(0.005)
    
    

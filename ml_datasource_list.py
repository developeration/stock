# coding:utf8


from pyspark.sql import SparkSession
import tushare  as ts
import settings.xsetting as xsetting
if __name__ == "__main__":
    spark = SparkSession.builder \
                .appName("ml_datasource_list") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', '/work/dev/settings/xsetting.py') \
                .getOrCreate()

    #.master("yarn") \
    sc = spark.sparkContext
    pro = ts.pro_api(xsetting.tosharekey())
    data_list = pro.stock_basic(exchange='', list_status='L',market='主板')
    values = data_list.values.tolist()
    columns = data_list.columns.tolist() 
    data_spark = spark.createDataFrame(values, columns)
    savepath = xsetting.stock_basic_path()+'SH'
    #xsetting.deletefile(sc,savepath)
    data_spark.write.mode("overwrite").format("json").save(savepath)
    print("ml_datasource_list Finished")

    
    
    
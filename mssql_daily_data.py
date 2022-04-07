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
import pymssql

spark = SparkSession.builder \
                .appName("ml_analyze_daily") \
                .master("local[*]") \
                .config('spark.submit.pyFiles', 'file:///work/dev/stock/settings/xsetting.py') \
                .getOrCreate()
sc = spark.sparkContext 
#filepath='file:///work/dev/stock/datasource/daily/000001.SZ'
#rf = spark.read.format("json").load(filepath)
#df = rf.toPandas()

daily_path=daily_path = xsetting.daily_path()
passwd = xsetting.mssql_passwd()
def mssqldailydata(filename):
    filepath = daily_path+filename
    rf = spark.read.format("json").load(filepath)
    print(filename)
    df = rf.toPandas()
    conn = pymssql.connect("localhost", "sa", passwd, "AIDB")
    with conn.cursor(as_dict=True) as cursor:

        _RowCount = df.shape[0]
        for i in range(0,_RowCount):
            #print(i)
            str_sql = """
            INSERT INTO [dbo].[daily]
            ([ts_code]
            ,[trade_date]
            ,[open]
            ,[high]
            ,[low]
            ,[close]
            ,[pre_close]
            ,[change]
            ,[pct_chg]
            ,[vol]
            ,[amount])
        VALUES
            ('{ts_code}'
            ,'{trade_date}'
            ,{open}
            ,{high}
            ,{low}
            ,{close}
            ,{pre_close}
            ,{change}
            ,{pct_chg}
            ,{vol}
            ,{amount})
            """.format(ts_code=df.iloc[i].ts_code,
            trade_date=df.iloc[i].trade_date,
            open=df.iloc[i].open,
            high=df.iloc[i].high,
            low=df.iloc[i].low,
            close=df.iloc[i].close,
            pre_close=df.iloc[i].pre_close,
            change=df.iloc[i].change,
            pct_chg=df.iloc[i].pct_chg,
            vol=df.iloc[i].vol,
            amount=df.iloc[i].amount)
            #print(str_sql)
            cursor.execute(str_sql) 
        conn.commit() 
    conn.close()

files_list = os.listdir(xsetting.daily_path())
df = pd.DataFrame(files_list)
x = df.applymap(mssqldailydata)
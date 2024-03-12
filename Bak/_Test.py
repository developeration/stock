import imp
from _Setting import StockSetting
import tushare  as ts
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col
from IPython import display

settings = StockSetting()

pro = ts.pro_api(settings.tushareKey)
data_daily_local = pro.moneyflow(ts_code="000001.SZ" )
#print(data_daily_local.count())
#data_daily_local.head()
print(data_daily_local.iloc[0])
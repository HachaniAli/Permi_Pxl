import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("example").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
#spark = SparkSession.builder.appName("example").getOrCreate()
spark.conf.set("spark.sql.sources.maxRecordsPerFile", -1)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

## Libraries
import pandas as pd
from pyspark.sql.functions import col, sum, date_format, concat, concat_ws, lit, regexp_replace, when, current_date,length, row_number, \
                                  udf, trim, expr, coalesce, to_date, year, lpad, month, lower,count, to_timestamp, col, sum as spark_sum, explode

from pyspark.sql.functions import col, array_contains, expr, udf,array_contains,substr

import pyspark.sql.functions as F                                  
from pyspark.sql.types import StringType, IntegerType, DateType  
import datetime as dt 
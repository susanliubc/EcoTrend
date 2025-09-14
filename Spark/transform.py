
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_date, date_sub, to_date, add_months, year, month, date_format
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
    

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-1080098782034-dnezjv50')


#data load
sp500 = spark.read.parquet("gs://de-eco-trend-bucket/SP500_data_us.parquet")
nasdaq100 = spark.read.parquet("gs://de-eco-trend-bucket/NASDAQ100_data_us.parquet")
dgs10 = spark.read.parquet("gs://de-eco-trend-bucket/DGS10_data_us.parquet")
vixcls = spark.read.parquet("gs://de-eco-trend-bucket/VIXCLS_data_us.parquet")
effr = spark.read.parquet("gs://de-eco-trend-bucket/EFFR_data_us.parquet")
cpiaucsl = spark.read.parquet("gs://de-eco-trend-bucket/CPIAUCSL_data_us.parquet")
pcepi = spark.read.parquet("gs://de-eco-trend-bucket/PCEPI_data_us.parquet")
civpart = spark.read.parquet("gs://de-eco-trend-bucket/CIVPART_data_us.parquet")
indpro = spark.read.parquet("gs://de-eco-trend-bucket/INDPRO_data_us.parquet")
csushpisa = spark.read.parquet("gs://de-eco-trend-bucket/CSUSHPISA_data_us.parquet")

# Get the date 5 years ago
five_years_ago = add_months(current_date(), -12 * 5)

# Filter each dataset to the last 5 years
sp500_filtered = sp500.filter(to_date(sp500['date'], 'yyyy-MM-dd') >= five_years_ago)
nasdaq100_filtered = nasdaq100.filter(to_date(nasdaq100['date'], 'yyyy-MM-dd') >= five_years_ago)
dgs10_filtered = dgs10.filter(to_date(dgs10['date'], 'yyyy-MM-dd') >= five_years_ago)
vixcls_filtered = vixcls.filter(to_date(vixcls['date'], 'yyyy-MM-dd') >= five_years_ago)
effr_filtered = effr.filter(to_date(effr['date'], 'yyyy-MM-dd') >= five_years_ago)

# Perform full outer joins to include all dates, with nulls for missing data
sp500_nasdaq100_dgs10_vixcls_effr_5years_daily = sp500_filtered \
    .join(nasdaq100_filtered, on='date', how='outer') \
    .join(dgs10_filtered, on='date', how='outer') \
    .join(vixcls_filtered, on='date', how='outer') \
    .join(effr_filtered, on='date', how='outer')

# add month and year
sp500_nasdaq100_dgs10_vixcls_effr_5years_daily = sp500_nasdaq100_dgs10_vixcls_effr_5years_daily \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("year", date_format("date", "yyyy")) \
    .withColumn("year_month", date_format("date", "yyyy-MM"))

# add categorical data for sp500
sp500_nasdaq100_dgs10_vixcls_effr_5years_daily = sp500_nasdaq100_dgs10_vixcls_effr_5years_daily.withColumn(
    'SP500_daily_change_category',
    F.when(F.col('SP500') > F.lag('SP500', 1).over(Window.orderBy('date')) * 1.01, 'Increase') \
     .when(F.col('SP500') < F.lag('SP500', 1).over(Window.orderBy('date')) * 0.99, 'Decrease') \
     .otherwise('Constant')
)

# Similarly transform the monthly series
cpiaucsl_filtered = cpiaucsl.filter(to_date(cpiaucsl['date'], 'yyyy-MM-dd') >= five_years_ago)
pcepi_filtered = pcepi.filter(to_date(pcepi['date'], 'yyyy-MM-dd') >= five_years_ago)
civpart_filtered = civpart.filter(to_date(civpart['date'], 'yyyy-MM-dd') >= five_years_ago)
indpro_filtered = indpro.filter(to_date(indpro['date'], 'yyyy-MM-dd') >= five_years_ago)
csushpisa_filtered = csushpisa.filter(to_date(csushpisa['date'], 'yyyy-MM-dd') >= five_years_ago)

cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly = cpiaucsl_filtered \
    .join(pcepi_filtered, on='date', how='outer') \
    .join(civpart_filtered, on='date', how='outer') \
    .join(indpro_filtered, on='date', how='outer') \
    .join(csushpisa_filtered, on='date', how='outer')

cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly = cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly \
    .withColumn("month", date_format("date", "MM")) \
    .withColumn("year", date_format("date", "yyyy")) \
    .withColumn("year_month", date_format("date", "yyyy-MM"))

# Perform left join by 'year_month' column to create the merged table
Ecotrend_merged = sp500_nasdaq100_dgs10_vixcls_effr_5years_daily \
    .join(cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly.drop('date', 'year', 'month'), on='year_month', how='left')

project_id = 'eco-trend-471821'
dataset_id = 'ecotrend_bq_dw'
daily_table_id = 'sp500_nasdaq100_dgs10_vixcls_effr_5years_daily' 
monthly_table_id = 'cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly'
merged_table_id = 'ecotrend_merged'

# Write the DataFrames to BigQuery
sp500_nasdaq100_dgs10_vixcls_effr_5years_daily \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'de-eco-trend-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{daily_table_id}') \
    .mode("overwrite") \
    .save()

cpiaucsl_pcepi_civpart_indpro_csushpisa_5years_monthly \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'de-eco-trend-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{monthly_table_id}') \
    .mode("overwrite") \
    .save()

Ecotrend_merged \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'de-eco-trend-bucket') \
    .option('table', f'{project_id}:{dataset_id}.{merged_table_id}') \
    .mode("overwrite") \
    .save()


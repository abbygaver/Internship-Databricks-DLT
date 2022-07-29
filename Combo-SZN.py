# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import *

# source = spark.conf.get("source")

@dlt.table(name="Bronze_rc",
                  comment = "New online retail customer data incrementally ingested from cloud object storage landing zone",
  table_properties={
    "quality": "bronze",
    "pipelines.cdc.tombstoneGCThresholdInSeconds": "2" #reducing the threshold to 2 instead of 5
  }
)
def Bronze_rc():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("s3://lakehouse-bronze-intern-2022/infiniture/retail_customers")
  )

# COMMAND ----------

expect_list = {"not_null_pk": "customer_id IS NOT NULL" } #, "QuantityNotNeg": "Quantity >= 0"}

@dlt.view(name="Bronze_rc_clean_v",
  comment="Cleansed bronze retail customer view (i.e. what will become Silver)")

# @dlt.expect("EMPLOYEE_ID", "EMPLOYEE_ID IS NOT NULL")
@dlt.expect_all(expect_list) 
# @dlt.expect("valid_address", "address IS NOT NULL")
# @dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")

def Bronze_rc_clean_v():
  return dlt.read_stream("Bronze_rc") \
  .withColumn('inputFileName',F.input_file_name()) \
  .withColumn('LoadDate',F.lit(datetime.now()))

# COMMAND ----------

dlt.create_target_table(name="Silver_rc",
  comment="Clean, merged retail sales data",
  table_properties={
    "quality": "silver",
    "pipelines.cdc.tombstoneGCThresholdInSeconds": "2"
  }
)

# COMMAND ----------

dlt.apply_changes(
  target = "Silver_rc", 
  source = "Bronze_rc_clean_v", 
  keys = ["customer_id"], 
  sequence_by = col("LoadDate"), 
  apply_as_deletes = expr("Op = 'D'"), 
  except_column_list = ["Op", "inputFileName", "LoadDate", "_rescued_data"])

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import *

# source = spark.conf.get("source")

@dlt.table(name="Bronze_or",
                  comment = "New online retail sales data incrementally ingested from cloud object storage landing zone",
  table_properties={
    "quality": "bronze",
    "pipelines.cdc.tombstoneGCThresholdInSeconds": "2" #reducing the threshold to 2 instead of 5
  }
)
def Bronze_or():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("s3://lakehouse-bronze-intern-2022/infiniture/online_retail")
  )

# COMMAND ----------

expect_list = {"not_null_pk": "id IS NOT NULL", "QuantityNotNeg": "Quantity >= 0"}

@dlt.view(name="Bronze_or_clean_v",
  comment="Cleansed bronze retail sales view (i.e. what will become Silver)")

# @dlt.expect("EMPLOYEE_ID", "EMPLOYEE_ID IS NOT NULL")
@dlt.expect_all(expect_list) 
# @dlt.expect("valid_address", "address IS NOT NULL")
# @dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")

def Bronze_or_clean_v():
  return dlt.read_stream("Bronze_or") \
  .withColumn('inputFileName',F.input_file_name()) \
  .withColumn('LoadDate',F.lit(datetime.now()))

# COMMAND ----------

dlt.create_target_table(name="Silver_or",
  comment="Clean, merged retail sales data",
  table_properties={
    "quality": "silver",
    "pipelines.cdc.tombstoneGCThresholdInSeconds": "2"
  }
)

# COMMAND ----------

dlt.apply_changes(
  target = "Silver_or", 
  source = "Bronze_or_clean_v", 
  keys = ["id"], 
  sequence_by = col("LoadDate"), 
  apply_as_deletes = expr("Op = 'D'"), 
  except_column_list = ["Op", "inputFileName", "LoadDate", "_rescued_data"])

# COMMAND ----------

# MAGIC %md
# MAGIC dlt.create_target_table(name="Bucket_Silver_2",
# MAGIC   comment="Clean, merged retail sales data",
# MAGIC    path="s3://lakehouse-silver-intern-2022/retail_customer/"
# MAGIC )
# MAGIC @dlt.view(name="Silver_rc_to_bucket",
# MAGIC   comment="Cleansed bronze retail sales view (i.e. what will become Silver)")
# MAGIC 
# MAGIC def Silver_rc_to_bucket():
# MAGIC   return dlt.read_stream("Silver_rc")
# MAGIC 
# MAGIC dlt.apply_changes(
# MAGIC   target = "Bucket_Silver_2", 
# MAGIC   source = "Silver_rc_to_bucket", 
# MAGIC   keys = ["customer_id"], 
# MAGIC   sequence_by = col("TIMESTAMP"))

# COMMAND ----------

# MAGIC %md
# MAGIC dlt.create_target_table(name="Bucket_Silver",
# MAGIC   comment="Clean, merged online retail  data",
# MAGIC    path="s3://lakehouse-silver-intern-2022/online_retail/"
# MAGIC )
# MAGIC @dlt.view(name="Silver_or_to_bucket",
# MAGIC   comment="Cleansed bronze retail sales view (i.e. what will become Silver)")
# MAGIC 
# MAGIC def Silver_or_to_bucket():
# MAGIC   return dlt.read_stream("Silver_or")
# MAGIC 
# MAGIC dlt.apply_changes(
# MAGIC   target = "Bucket_Silver", 
# MAGIC   source = "Silver_or_to_bucket", 
# MAGIC   keys = ["CustomerID"], 
# MAGIC   sequence_by = col("TIMESTAMP"))

# COMMAND ----------

@dlt.table(name="Gold_or", comment="A table containing the top 10 unit prices + quantity.")
def Gold_or():
    return (
    dlt.read("Silver_or")
#       .filter(expr("current_page_title == 'Apache_Spark'"))
#       .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("LIVE.Silver_or.CustomerID"))
      .select("LIVE.Silver_or.UnitPrice", "LIVE.Silver_or.Quantity", "LIVE.Silver_or.CustomerID")
      #.limit(10)
  )

# COMMAND ----------

@dlt.table(name="Gold_rc", comment="A table containing the top 10 customer names.")
def Gold_rc():
    return (
    dlt.read("Silver_rc")
#       .filter(expr("current_page_title == 'Apache_Spark'"))
#       .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("LIVE.Silver_rc.customer_id"))
      .select("LIVE.Silver_rc.full_name", "LIVE.Silver_rc.phone_num", "LIVE.Silver_rc.customer_id")
      #.limit(10)
  )

# COMMAND ----------

# Joining the two Silver Tables by calling them by the "function" name
@dlt.table(
  comment="Joining Gold Tables"
)
def my_gold_table():
    gold_one = dlt.read("Gold_rc")
    gold_two = dlt.read("Gold_or")
    return ( 
        gold_one.join(gold_two, gold_one.customer_id == gold_two.CustomerID, how="inner")
    )

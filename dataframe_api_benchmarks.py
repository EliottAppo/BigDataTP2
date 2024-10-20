
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col
from pyspark.sql.functions import sum as spark_sum
from contextlib import contextmanager

import sys
import os
import time
import logging

@contextmanager
def time_usage(name=""):
    '''
    Logs the time usage in a code block.
    
    Parameters
    -----------

    name : The label attached to the code block.
    '''
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float("%.4f" % (end - start))
    logging.info('%s: elapsed seconds: %s', name, elapsed_seconds)
    
# Sets logging, so that the execution time of the queries will be visible.
logging.getLogger().setLevel(logging.INFO)

# The input CSV file name must be specified at the command line when executing this Python program.
# If not, an error is raised.
if len(sys.argv) != 2:
    print("Specify the input CSV file on the command line")
    sys.exit(-1)

# Initialization of the SparkSession.
spark = (SparkSession\
         .builder\
         .appName("Benchmark DataFrame API on large CSV files")\
         .getOrCreate())

# Disable most of the Spark logging
spark.sparkContext.setLogLevel("ERROR")

#############################################################
# MODIFY THIS VALUE
# This is the path to the HDFS folder that contains 
# the input CSV files.
# 
# REPLACE sarXX WITH EITHER sar01 OR sar17.
############################################################# 
input_path = "hdfs://sar01:9000/data/sales/"


# Obtain the name of the input CSV file (one of the files that contains the table store_sales).
csvfile_name = os.path.basename(sys.argv[1])

# Obtain the absolute path to the input CSV file.
store_sales_file = input_path + csvfile_name

# The file containing the table customer.
customer_file = input_path + "customer_10000.dat"

# Reads the input files and log the execution time.
with time_usage("Read input files "):

    # Reads a sample of the input file with the table store_sales to infer the schema.
    df_store_sales_schema = spark.read.csv(store_sales_file, header=True, samplingRatio=0.01, inferSchema=True).schema
    # Reads the whole input file with the inferred schema.
    df_store_sales = spark.read.csv(store_sales_file, df_store_sales_schema, header=True)


    # Reads a sample of the input file with the table customer to infer the schema.
    df_customer_schema = spark.read.csv(customer_file, header=True, samplingRatio=0.01, inferSchema=True).schema
    # Reads the whole input file with the inferred schema.
    df_customer = spark.read.csv(customer_file, df_customer_schema, header=True)

# Caching the input DataFrames
df_store_sales.cache()
df_customer.cache()

'''
Benchmark the API with five queries.
Each query is executed 5 times, the reason being that the execution time
stabilizes after some iterations.
'''
#####################################
# COMPLETE THE FOLLOWING CODE
#####################################
for i in range(0, 5):
    print('-------------------------------------------')
    # Q1. SELECT count(*) FROM customer
    with time_usage("Query q1 (iteration {})".format(i)):
        q1_result = df_customer.count()
        print("Number of customers: {}".format(q1_result))

    # Q2. SELECT max(ss_list_price) FROM store_sales
    with time_usage("Query q2 (iteration {})".format(i)):
        q2_result = df_store_sales.agg(max(col("ss_list_price")).alias("max_price"))
        q2_result.show(n=1)

    # Q3. SELECT ss_customer_sk, SUM(ss_net_paid_inc_tax) as amountSpent 
    #     FROM store_sales 
    #     GROUP BY ss_customer_sk 
    with time_usage("Query q3 (iteration {})".format(i)):
        q3_result = df_store_sales.groupBy("ss_customer_sk").agg(spark_sum(col("ss_net_paid_inc_tax").cast("float")).alias("amountSpent"))
        q3_result.show(n=1)

    # Q4. SELECT ss_customer_sk, SUM(ss_net_paid_inc_tax) as amountSpent 
    #     FROM store_sales 
    #     GROUP BY ss_customer_sk 
    #     ORDER BY  amountSpent DESC
    with time_usage("Query q4 (iteration {})".format(i)):
        q4_result = df_store_sales.groupBy("ss_customer_sk").agg(spark_sum("ss_net_paid_inc_tax").alias("amountSpent")).orderBy("amountSpent", ascending=False)
        q4_result.show(n=1)

    # Q5. SELECT c.c_first_name, c.c_last_name, SUM(ss_net_paid_inc_tax) as amountSpent 
    #     FROM store_sales s JOIN customer c ON s.ss_customer_sk = c.c_customer_sk 
    #     GROUP BY ss_customer_sk 
    #     ORDER BY  amountSpent DESC
    with time_usage("Query 5 (iteration {})".format(i)):
        q5_result = df_store_sales.join(df_customer, df_store_sales.ss_customer_sk == df_customer.c_customer_sk) \
        .groupBy("c_first_name", "c_last_name") \
        .agg(spark_sum("ss_net_paid_inc_tax").alias("amountSpent")) \
        .orderBy("amountSpent", ascending=False)
        q5_result.show(n=1)
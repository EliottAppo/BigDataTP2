'''
Computation of the average temperature per year using the DataFrame API.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import sys
import os
import time
import logging
from contextlib import contextmanager
import pandas as pd

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
         .appName("Avg temperatures with dataframes")\
         .getOrCreate())

# Disable most of the Spark logging
spark.sparkContext.setLogLevel("ERROR")

#############################################################
#
# IMPLEMENT THIS FUNCTION. 
# The function takes in a dataframe with the input data and
# returns a  new dataframe with the average temperatures 
# per year.
#
#############################################################
def avg_temperature_df(df):
    '''
    Returns the average temperature per year.

    Parameters
    -----------
    df : DataFrame with the input data.

    Returns
    --------
    A new DataFrame that contains the average temperature per year.
    '''

    result = df.groupBy("_c0").agg((avg("_c6").alias("avg_temperature")))
    result.show()
    return result



#############################################################
# MODIFY THIS VALUE
# This is the path to the HDFS folder that contains 
# the input CSV files.
# 
# REPLACE sarXX WITH EITHER sar01 OR sar17.
#############################################################

input_path = "hdfs://sar01:9000/data/temperatures/"


#############################################################
# MODIFY THIS VALUE
# This is the path to the output HDFS folder that contains 
# the output files.
# 
# REPLACE sarXX WITH EITHER sar01 OR sar17.
# SPECIFY THE PATH TO YOUR FOLDER ON HDFS.
#############################################################
output_path = "hdfs://sar01:9000/sdim/sdim_18/"

# Get the name of the input CSV file
csvfile_name = os.path.basename(sys.argv[1])

# Get the absolute path to the input CSV file.
input_file = input_path + csvfile_name

# Sets the path to the output folder.
output_folder = output_path + os.path.splitext(csvfile_name)[0] + ".df.out"

#########################################################################
#
# COMPLETE THIS INSTRUCTION TO READ THE INPUT CSV FILE INTO A DATAFRAME.
# MAKE SURE YOU  SPECIFY THE SCHEMA OF THE DATAFRAME.
#
#########################################################################       


df = spark.read.csv(input_file, header=False, inferSchema=True)

# Call the function to compute the average temperature per year
df_avg = avg_temperature_df(df)
df_avg.cache()

# Prints the output DataFrame to HDFS.
with time_usage("Execution time of first action df_avg.write.csv"):
    df_avg.write.csv(output_folder)

# Prints the first 5 rows to the terminal.
with time_usage("Execution time of second action df_avg.write.csv"):
    df_avg.write.csv(output_folder+".bis")
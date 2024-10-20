'''
Computation of the average temperature per year using SQL on a view.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import sys
import os
import time
import logging
from contextlib import contextmanager

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
         .appName("Avg temperatures with SQL (view)")\
         .getOrCreate())

# Disable most of the Spark logging
spark.sparkContext.setLogLevel("ERROR")

#############################################################
#
# IMPLEMENT THIS FUNCTION. 
# The function takes in a dataframe with the input data and 
# the name to be given to the view and
# returns a  new dataframe with the average temperatures 
# per year.
# The function must use SQL code on a view.
#
#############################################################
def avg_temperature_sql(df, view_name):
    '''
    Returns a new dataframe with the average temperature per year.

    Parameters
    -----------

    df: dataframe with the input data.
    view_name: the name to be given to the view.


    Returns
    --------
    A new dataframe with the average temperature per year.

    '''
    # Create the view: 
    # NOTE: THE NAME OF THE VIEW IS IN THE ARGUMENT view_name OF THIS FUNCTION
    temperature_view = df.createOrReplaceTempView(view_name)
    
    ## COMPLETE THE CODE TO RETURN A NEW DATAFRAME WITH THE AVERAGE 
    # TEMPERATURE PER YEAR, BY USING SQL CODE ON THE VIEW.

    query = f"""
    SELECT _c0 AS Year, AVG(_c6) AS avg_temperature
    FROM {view_name}
    GROUP BY YEAR
    ORDER BY YEAR
    """

    avg_temperatures = spark.sql(query)

    return avg_temperatures



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

# Obtain the name of the input CSV file.
csvfile_name = os.path.basename(sys.argv[1])

# Obtain the absolute path to the input CSV file.
input_file = input_path + csvfile_name

# Path to the output folder.
output_folder = output_path + os.path.splitext(csvfile_name)[0] + ".sql.out"

#########################################################################
#
# COMPLETE THIS INSTRUCTION TO READ THE INPUT CSV FILE INTO A DATAFRAME.
# MAKE SURE TO SPECIFY THE SCHEMA OF THE DATAFRAME.
#
#########################################################################           
df = spark.read.csv(input_file)
            
# Compute the dataframe with the average temperature per year.
df = avg_temperature_sql(df, os.path.splitext(csvfile_name)[0])
df.show()

# Write the obtained dataframe to the output directory.
with time_usage("Execution time of the action df_avg.write.csv"):
    df.write.csv(output_folder)


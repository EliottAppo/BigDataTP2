'''
Computation of the average temperature per year using RDDs
'''

import sys
import os
import time
import logging
from pyspark import SparkContext

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

# Fast MapReduce code to compute the average temperature per year in Spark using RDDs
def avg_temperature_fast(theText_file):
    '''
    Returns an RDD where each item is a key-value pair, the key being the year and 
    the value the average temperature for the year.

    Parameters
    ----------
    theText_file : Input RDD, each item is a string with a temperature measurement.

    Returns
    ---------
    A new RDD where each item is a key-value pair, the key being the year and 
    the value the average temperature for the year.

    '''
    temperatures = theText_file           \
       .map(lambda line: line.split(",")) \
       .map(lambda term: (term[0],   (float(term[6]), 1))) \
       .reduceByKey(lambda x, y: (x[0]+y[0], x[1] + y[1])) \
       .mapValues(lambda x: x[0]/x[1])
       
    return temperatures

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

# Get the name of the input CSV file.
csvfile_name = os.path.basename(sys.argv[1])

# Get the absolute path to the input file.
input_file = input_path + csvfile_name

# Set the name of the output directory.
output_folder = output_path + os.path.splitext(csvfile_name)[0]+".rdd.out"

# Create the Spark context and open the input file as a RDD.
sc = SparkContext()
# Disable most of the Spark logging
sc.setLogLevel("ERROR")
text_file = sc.textFile(input_file)

# Print the number of partitions of the input RDD.
print("Number of partitions of the input RDD: {}".format(text_file.getNumPartitions()))

# Run the code to compute the average temperature.
temperatures = avg_temperature_fast(text_file)

# Print the number of partitions of the output RDD.
print("Number of partitions of the output RDD: {}".format(temperatures.getNumPartitions()))

# Save the output RDD to the output folder in HDFS
with time_usage("Save as text file execution time"):
    temperatures.saveAsTextFile(output_folder)

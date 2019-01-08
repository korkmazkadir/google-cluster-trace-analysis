import sys
from pyspark import SparkContext
import time


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("../clusterdata-2011-2/machine_attributes/part-00000-of-00001.csv.gz")


print("Number of machine attributes: {}".format(wholeFile.count()))

# Display the 5 first nationalities
print("A few examples of machine attributes:")
for elem in wholeFile.take(5):
	print(elem)


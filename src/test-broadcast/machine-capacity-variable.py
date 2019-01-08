import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
wholeFile = sc.textFile("../../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
machineEvents = tokens.map(lambda p: Row( time=int(p[0]), machine_id=int(p[1]), event_type=int(p[2]), platform_id=p[3], cpu=float(p[4] or 0) , memory=float(p[5] or 0 ) ))
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)

def addToMachineCapacityDictionary(machineEvent):
    machine_capacity_dictionary[machineEvent.machine_id] = [ machineEvent.cpu , machineEvent.memory ]

machine_capacity_dictionary = schemaMachineEvents.select("machine_id", "cpu", "memory" ).rdd.map(lambda machineEvent: ( machineEvent.machine_id, [machineEvent.cpu, machineEvent.memory] )).collectAsMap()

print("Size of dictionary : " + str(sys.getsizeof(machine_capacity_dictionary)))

print("Printing the dictionar")
#print(machine_capacity_dictionary)

#distinctMachineEventsCount = machineEvents.count()
#print("Distinct machine event count : {}".format(distinctMachineEventsCount))

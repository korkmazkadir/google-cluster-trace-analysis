import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_MACHINE_UPDATE_EVENTS = '''
SELECT 
    distinct
    machine_id,
    platform_id,
    cpu,
    memory
FROM macine_events WHERE event_type = 2
'''

SELECT_MACHINE_UPDATE_COUNTS = '''
SELECT 
    machine_id,
    count(*) as update_count
FROM machine_update_events
GROUP BY machine_id
ORDER BY update_count DESC
'''


SELECT_THE_MACHINE_UPDATE_EVENTS = '''
SELECT 
    time,
    machine_id,
    event_type,
    platform_id,
    cpu,
    memory
FROM macine_events WHERE machine_id = "3349480405" ORDER BY time ASC
'''


SELECT_MACHINE_REMOVE_EVENTS = '''
SELECT 
    time,
    machine_id,
    platform_id,
    cpu,
    memory
FROM macine_events 
WHERE event_type = 1
ORDER BY time, platform_id
'''



#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
wholeFile = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
machineEvents = tokens.map(lambda p: Row( time=int(p[0]), machine_id=int(p[1]), event_type=int(p[2]), platform_id=p[3], cpu=float(p[4] or 0) , memory=float(p[5] or 0 ) ))

# Infer the schema, and register the macine_event as a table.
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)
schemaMachineEvents.registerTempTable("macine_events")

machineUpdateEevents = sqlContext.sql(SELECT_MACHINE_UPDATE_EVENTS)
machineUpdateEevents.registerTempTable("machine_update_events")

machineUpdateEevents.show();

print("machine update event count : {}".format(machineUpdateEevents.count()))



machineUpdateCounts = sqlContext.sql(SELECT_MACHINE_UPDATE_COUNTS)
machineUpdateCounts.show(100);
print("machine update count : {}".format(machineUpdateCounts.count()))


theMachineUpdateEvents = sqlContext.sql(SELECT_THE_MACHINE_UPDATE_EVENTS)
theMachineUpdateEvents.show(100);


print("Writing to the csv file...")

#writeToCSV(machineDistAccordingToCPUCapacity,"machine-distribution-according-to-cpu-capacity.csv")
#writeToCSV(machineDistAccordingToMemorCapacity,"machine-distribution-according-to-memory-capacity.csv")
#writeToCSV(machineDistAccordingToPlatform,"machine-distribution-according-to-platform.csv")

print("End of write.")

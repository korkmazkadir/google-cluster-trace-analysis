import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_REMOVE_EVENTS = '''
SELECT 
  *
FROM macine_events 
WHERE event_type = 1
ORDER BY time asc
'''


SELECT_ORDERED_MACHINE_EVENTS = '''
SELECT 
  *
FROM macine_events 
ORDER BY time asc
'''

SELECT_LAST_EVENTS  = '''
SELECT *
FROM
(
  SELECT 
    machine_id, last(event_type) as last_event, last(time) as last_event_time
  FROM ordered_machine_events
  GROUP BY machine_id
) WHERE last_event = 1
'''


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[*]")
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

#removeEvents = sqlContext.sql(SELECT_REMOVE_EVENTS).cache()
#removeEvents.registerTempTable("remove_events")


orderedMachineEvents = sqlContext.sql(SELECT_ORDERED_MACHINE_EVENTS).cache()
orderedMachineEvents.registerTempTable("ordered_machine_events")

lastevents = sqlContext.sql(SELECT_LAST_EVENTS).cache()

print("Last events count : {}".format(lastevents.count()))

#removeEvents.show(100,False);
#print("Remove event count : {}".format(removeEvents.count()))




print("Writing to the csv file...")

#writeToCSV(removeEvents,"remove-event.csv")
writeToCSV(lastevents,"last-events.csv")

print("End of write.")

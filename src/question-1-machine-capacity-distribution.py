import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_DISTINCT_MACHINE_EVENTS = '''
SELECT 
    first(time) as time,
    first(machine_id) as machine_id,
    first(event_type) as event_type,
    first(platform_id) as platform_id,
    first(cpu) as  cpu ,
    first(memory) as memory
FROM (
    select * from macine_events order by time desc
) me
GROUP BY machine_id
'''

SELECT_MACHINE_DIST_ACCORDING_TO_CPU = '''
SELECT 
    distinct cpu as cpu_capacity, 
    count(*) as number_of_machines
FROM distinct_machine_events 
GROUP BY cpu
ORDER BY cpu_capacity DESC
'''

SELECT_MACHINE_DIST_ACCORDING_TO_MEMORY = '''
SELECT 
    distinct memory as memory_capacity, 
    count(*) as number_of_machines 
FROM distinct_machine_events 
GROUP BY memory
ORDER BY memory_capacity DESC
'''

SELECT_MACHINE_DIST_ACCORDING_TO_PLATFORM = '''
SELECT 
    distinct platform_id as platform_id, 
    count(*) as number_of_machines 
FROM distinct_machine_events 
GROUP BY platform_id
ORDER BY platform_id DESC
'''

SELECT_MACHINE_COUNT = '''
SELECT 
    count(*) as machine_count
FROM distinct_machine_events
ORDER
'''

SELECT_MACHINE_DIST_ACCORDING_TO_CAPACITY = '''
SELECT 
    distinct cpu, memory,
    (cpu + memory) / 2 as capacity,
    count(*) as number_of_machines,
    round(count(*) * 100 / (select count(*) from distinct_machine_events),2) as machine_percentage
FROM distinct_machine_events
GROUP BY cpu, memory
ORDER BY capacity DESC
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

distinctMachineEvents = sqlContext.sql(SELECT_DISTINCT_MACHINE_EVENTS)
distinctMachineEvents.registerTempTable("distinct_machine_events")


machineDistAccordingToCPUCapacity = sqlContext.sql(SELECT_MACHINE_DIST_ACCORDING_TO_CPU).cache()
machineDistAccordingToMemorCapacity = sqlContext.sql(SELECT_MACHINE_DIST_ACCORDING_TO_MEMORY).cache()
machineDistAccordingToPlatform = sqlContext.sql(SELECT_MACHINE_DIST_ACCORDING_TO_PLATFORM).cache()


machineCount = sqlContext.sql(SELECT_MACHINE_COUNT).cache()
machineDistAccordingToCapacity = sqlContext.sql(SELECT_MACHINE_DIST_ACCORDING_TO_CAPACITY).cache()


machineDistAccordingToCPUCapacity.show()
machineDistAccordingToMemorCapacity.show()
machineDistAccordingToPlatform.show()
machineDistAccordingToCapacity.show()

distinctMachineEventsCount = distinctMachineEvents.count()
print("Distinct machine event count : {}".format(distinctMachineEventsCount))


print("Writing to the csv file...")

writeToCSV(machineDistAccordingToCPUCapacity,"machine-distribution-according-to-cpu-capacity.csv")
writeToCSV(machineDistAccordingToMemorCapacity,"machine-distribution-according-to-memory-capacity.csv")
writeToCSV(machineDistAccordingToPlatform,"machine-distribution-according-to-platform.csv")
writeToCSV(machineDistAccordingToCapacity,"machine-distribution-according-to-capacity.csv")

print("End of write.")

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_UNIQUE_MACHINE_ADD_EVENTS = '''
SELECT 
    distinct
    machine_id,
    platform_id,
    cpu,
    memory
FROM macine_events WHERE event_type = 0
'''


SELECT_TASK_SCHEDULE_EVENTS = '''
SELECT  * 
FROM task_events
WHERE event_type = 1
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



wholeFile = sc.textFile("../clusterdata-2011-2/task_events/")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
taskEvents = tokens.map(lambda p: Row( 
                        
                        time=int(p[0]), 
                        missing_info=int(p[1] or 0), 
                        job_id=int(p[2]), 
                        task_index=int(p[3]), 
                        machine_id=int(p[4] or 0),
                        event_type=int(p[5] or 0 ),
                        user=p[6],
                        scheduling_class=int(p[7] or 0 ),
                        priority=int(p[8] or 0 ),
                        cpu_request=float(p[9] or 0 ),
                        memory_request=float(p[10] or 0 ),
                        disk_space_request=float(p[11] or 0 ),
                        different_machine_restrictions=bool(p[12] or 0 ),
))


taskEvents = sqlContext.createDataFrame(taskEvents)
taskEvents.registerTempTable("task_events")


schemaTaskScheduleEvents = sqlContext.sql(SELECT_TASK_SCHEDULE_EVENTS)
schemaTaskScheduleEvents.registerTempTable("task_schedule_events")

schemaTaskScheduleEvents.show();


# Infer the schema, and register the macine_event as a table.
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)
schemaMachineEvents.registerTempTable("macine_events")

uniqueAddEvents = sqlContext.sql(SELECT_UNIQUE_MACHINE_ADD_EVENTS)
uniqueAddEvents.registerTempTable("unique_machine_add_events")

uniqueAddEvents.show();

print("machine add event count : {}".format(uniqueAddEvents.count()))


taskMachineData = uniqueAddEvents.alias("u").join(schemaTaskScheduleEvents.alias("s"), uniqueAddEvents.machine_id == schemaTaskScheduleEvents.machine_id )   .select("u.cpu","u.memory","s.job_id","s.task_index","s.priority","s.cpu_request","s.memory_request");

#taskMachineData.show();


print("Writing to the csv file...")

writeToCSV(taskMachineData,"task-machine-data.csv")
#writeToCSV(machineDistAccordingToMemorCapacity,"machine-distribution-according-to-memory-capacity.csv")
#writeToCSV(machineDistAccordingToPlatform,"machine-distribution-according-to-platform.csv")

print("End of write.")

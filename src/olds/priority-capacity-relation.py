import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))



SELECT_TASK_SCHEDULE_EVENTS = '''
SELECT  * 
FROM task_events
WHERE event_type = 1
'''

SELECT_TASK_MACHINE_DATA= '''
SELECT
    te.priority,
    me.machine_id,
    me.cpu,
    me.memory,
    (me.cpu + me.memory) / 2 as capacity
FROM (

    SELECT 
        first(time) as time,
        first(machine_id) as machine_id,
        first(platform_id) as platform_id,
        first(cpu) as  cpu ,
        first(memory) as memory,
        (first(cpu) + first(memory)) / 2 as capacity
    FROM (
        select * from macine_events order by time desc
    ) me
    GROUP BY machine_id
    
) me 
INNER JOIN (
    
    SELECT 
    distinct machine_id, priority
    FROM task_events WHERE event_type = 1
    
) te on te.machine_id = me.machine_id

'''

SELECT_CPU_PRIORTY_RELATION= '''
SELECT
    priority,
    cpu,
    count(*) as count
FROM task_machine_data
GROUP BY priority,cpu
ORDER BY priority,cpu
'''

SELECT_MEMORY_PRIORTY_RELATION= '''
SELECT
    priority,
    memory,
    count(*) as count
FROM task_machine_data
GROUP BY priority,memory
ORDER BY priority,memory
'''

SELECT_CAPACITY_PRIORTY_RELATION= '''
SELECT
    priority,
    capacity,
    count(*) as count
FROM task_machine_data
GROUP BY priority,capacity
ORDER BY priority,capacity
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



wholeFile = sc.textFile("../clusterdata-2011-2/task_events")

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

'''
taskMachineData = uniqueAddEvents.alias("u").join(schemaTaskScheduleEvents.alias("s"), uniqueAddEvents.machine_id == schemaTaskScheduleEvents.machine_id )   .select("u.cpu","u.memory","u.capacity","s.job_id","s.task_index","s.priority","s.cpu_request","s.memory_request");

taskMachineData.show();
'''
taskMachineData = sqlContext.sql(SELECT_TASK_MACHINE_DATA).cache()
taskMachineData.registerTempTable("task_machine_data")
taskMachineData.show();

cpuPriorityRelation = sqlContext.sql(SELECT_CPU_PRIORTY_RELATION).cache()
cpuPriorityRelation.show() 

memoryPriorityRelation = sqlContext.sql(SELECT_MEMORY_PRIORTY_RELATION).cache()
memoryPriorityRelation.show() 

capacityPriorityRelation = sqlContext.sql(SELECT_CAPACITY_PRIORTY_RELATION).cache()
capacityPriorityRelation.show() 

print("Writing to the csv file...")

writeToCSV(cpuPriorityRelation,"priority-cpu.csv")
writeToCSV(memoryPriorityRelation,"priority-memory.csv")
writeToCSV(capacityPriorityRelation,"priority-capacity.csv")

print("End of write.")

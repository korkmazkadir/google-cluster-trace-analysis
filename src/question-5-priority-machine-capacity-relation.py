import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_TASK_SCHEDULE_EVENTS = '''
SELECT machine_id, priority
FROM task_events
WHERE event_type = 1
'''

SELECT_DISTINCT_MACHINE_EVENTS = '''
SELECT 
    first(machine_id) as machine_id,
    first(cpu) as  cpu ,
    first(memory) as memory,
    (first(cpu) + first(memory)) / 2 as capacity
FROM (
    select * from macine_events order by time desc
) me
GROUP BY machine_id
'''


SELECT_PRIORITY_CAPACITY_RELATION = '''
SELECT 
  priority,
  first(cpu),
  first(memory),
  capacity,
  count(*) as task_count
FROM
  (
	SELECT machine_id, priority
	FROM task_events
	WHERE event_type = 1
  ) tse
INNER JOIN 
  (
	SELECT 
	    first(machine_id) as machine_id,
	    first(cpu) as  cpu ,
	    first(memory) as memory,
	    (first(cpu) + first(memory)) / 2 as capacity
	FROM (
	    select * from macine_events order by time desc
	) me
	GROUP BY machine_id
  ) tme on tse.machine_id= tme.machine_id
GROUP BY
  priority,capacity
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


#schemaTaskScheduleEvents = sqlContext.sql(SELECT_TASK_SCHEDULE_EVENTS).cache()
#schemaTaskScheduleEvents.registerTempTable("task_schedule_events")

#schemaTaskScheduleEvents.show();

schemaMachineEvents = sqlContext.createDataFrame(machineEvents)
schemaMachineEvents.registerTempTable("macine_events")


#distinctMachineEvents = sqlContext.sql(SELECT_DISTINCT_MACHINE_EVENTS).cache()
#distinctMachineEvents.registerTempTable("distinct_machine_events")


priorityCapacityRelation =  sqlContext.sql(SELECT_PRIORITY_CAPACITY_RELATION).cache()

priorityCapacityRelation.show()

print("Writing to the csv file...")

writeToCSV(priorityCapacityRelation,"priority-capacity-relation.csv")

print("End of write.")

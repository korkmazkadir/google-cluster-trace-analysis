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


SELECT_TASK_SCHEDULE_COUNTS = '''
SELECT 
  machine_id, 
  count(*) as scheduled_task_count
FROM task_events
WHERE event_type = 4
GROUP BY machine_id
'''


SELECT_MACHINE_UTILIZATION = '''
SELECT 
  dme.machine_id,
  dtse.scheduled_task_count
FROM distinct_machine_events dme
LEFT JOIN distinct_task_schedule_events dtse on dtse.machine_id =  dme.machine_id
ORDER BY dtse.scheduled_task_count asc
'''


#### Driver program
import pyspark
conf = pyspark.SparkConf()
conf.set("spark.driver.memory", "11g")
conf.setMaster("local[*]")

# start spark with 1 worker thread
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)


###################################################################################################################################
# read the input file into an RDD[String]
wholeFile = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
machineEvents = tokens.map(lambda p: Row( time=int(p[0]), machine_id=int(p[1]), event_type=int(p[2]), platform_id=p[3], cpu=float(p[4] or 0) , memory=float(p[5] or 0 ) ))

# Infer the schema, and register the macine_event as a table.
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)
schemaMachineEvents.registerTempTable("macine_events")

###################################################################################################################################

#wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz")
wholeFile = sc.textFile("../clusterdata-2011-2/task_events")

# split each line into an array of items
tokens = wholeFile.map(lambda x: x.split(','))

# creates columns
taskEvents = tokens.map(lambda p: Row(

  time=int(p[0]),
  missing_info=int(p[1] or 0),
  job_id=int(p[2]),
  task_index=int(p[3]),
  machine_id=int(p[4] or 0),
  event_type=int(p[5] or 0),
  user=p[6],
  scheduling_class=int(p[7] or 0),
  priority=int(p[8] or 0),
  cpu_request=float(p[9] or 0),
  memory_request=float(p[10] or 0),
  disk_space_request=float(p[11] or 0),
  different_machine_restrictions=bool(p[12] or 0),
))

# Infer the schema, and register the macine_event as a table.
schemaTaskEvents = sqlContext.createDataFrame(taskEvents)
schemaTaskEvents.registerTempTable("task_events")

#########################################################################################################################################


distinctMachineEvents = sqlContext.sql(SELECT_DISTINCT_MACHINE_EVENTS)
distinctMachineEvents.registerTempTable("distinct_machine_events")


distinctTaskScheduleEvents = sqlContext.sql(SELECT_TASK_SCHEDULE_COUNTS)
distinctTaskScheduleEvents.registerTempTable("distinct_task_schedule_events")


machineUtilization = sqlContext.sql(SELECT_MACHINE_UTILIZATION).cache()


count = machineUtilization.count()
print("Distinct machine event count : {}".format(count))


machineUtilization.show(100,False);

print("Writing to the csv file...")

writeToCSV(machineUtilization,"machine-utilization.csv")

print("End of write.")

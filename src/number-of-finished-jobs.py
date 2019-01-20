import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


# def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
    dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save(
        "../output/".format(fileName))




SELECT_FINISHED_TASKS = '''
SELECT 
    finished.job_id,
    finished.task_index,
    finished.cpu_request,
    finished.memory_request,
    finished.disk_space_request,
    scheduled.time as start_time,
    finished.time as end_time,
    floor((finished.time - scheduled.time)/1000000) as elapsed_time
    
FROM (
    SELECT
        job_id,
        task_index,
        max(time) as time,
        first(cpu_request) as cpu_request,
        first(memory_request) as memory_request,
        first(disk_space_request) as disk_space_request
    FROM task_events
    WHERE event_type = 4
    GROUP BY job_id, task_index
) finished
INNER JOIN(
    SELECT
        job_id,
        task_index,
        max(time) as time
    FROM task_events
    WHERE event_type = 1
    GROUP BY job_id, task_index
) scheduled on scheduled.job_id = finished.job_id and scheduled.task_index = finished.task_index 
WHERE (finished.time - scheduled.time) < 0
'''

#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[16]")
sc.setLogLevel("ERROR")

# creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz")

#wholeFile = sc.textFile("../clusterdata-2011-2/task_events")

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


finishedTasks = sqlContext.sql(SELECT_FINISHED_TASKS).cache()

finishedTasks.show(200,False)

print("Number of jobs : {}".format(finishedTasks.count()))

print("Writing to the csv file...")

writeToCSV(finishedTasks, "job-task-count.csv")

print("End of write.")

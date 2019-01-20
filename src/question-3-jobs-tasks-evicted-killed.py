import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_JOB_COUNTS = '''
SELECT 
    sum(if(event_type = 1, 1, 0)) as scheduled_job_count,
    sum(if(event_type = 2, 1, 0)) as evicted_job_count,
    sum(if(event_type = 5, 1, 0)) as killed_job_count
FROM job_events
'''

SELECT_TASK_COUNTS = '''
SELECT 
    sum(if(event_type = 1, 1, 0)) as scheduled_task_count,
    sum(if(event_type = 2, 1, 0)) as evicted_task_count,
    sum(if(event_type = 5, 1, 0)) as killed_task_count
FROM task_events
'''


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)


############################################################################################

#wholeFile = sc.textFile("../clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz")
wholeFile = sc.textFile("../clusterdata-2011-2/job_events")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
jobEvents = tokens.map(lambda p: Row(
                        time=int(p[0]),
                        missing_info=int(p[1] or 0),
                        job_id=int(p[2]),
                        event_type=int(p[3] or 0),
                        user=p[4],
                        scheduling_class=int(p[5] or 0),
                        job_name=p[6],
                        logical_job_name=p[7]
))


# Infer the schema, and register the macine_event as a table.
schemaJobEvents = sqlContext.createDataFrame(jobEvents)
schemaJobEvents.registerTempTable("job_events")

#############################################################################################

#wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz")
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



# Infer the schema, and register the macine_event as a table.
schemaTaskEvents = sqlContext.createDataFrame(taskEvents)
schemaTaskEvents.registerTempTable("task_events")

############################################################################################


jobCounts = sqlContext.sql(SELECT_JOB_COUNTS).cache()
taskCounts = sqlContext.sql(SELECT_TASK_COUNTS).cache()

print("Printing Job Event statistics :")
jobCounts.show()

print("Printing Task Event statistics : ")
taskCounts.show()


print("Writing to the csv file...")

writeToCSV(jobCounts,"job-task-count.csv")
writeToCSV(taskCounts,"job-task-count.csv")

print("End of write.")

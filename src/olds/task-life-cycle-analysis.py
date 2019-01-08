import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


SELECT_JOB_TASK_COUNT = '''
SELECT 
    priority,
    
    sum(if(event_type = 2, 1, 0)) as evicted_task_count,
    sum(if(event_type = 3, 1, 0)) as failed_task_count,
    sum(if(event_type = 5, 1, 0)) as killed_task_count,
    sum(if(event_type = 6, 1, 0)) as lost_task_count,
    
    sum(if(event_type = 0, 1, 0)) as submitted_task_count,
    sum(if(event_type = 1, 1, 0)) as scheduled_task_count,
    sum(if(event_type = 4, 1, 0)) as finished_task_count,
    
    sum(if(event_type = 7, 1, 0)) as update_pending_task_count,
    sum(if(event_type = 8, 1, 0)) as update_running_task_count
    
FROM task_events
GROUP BY priority
ORDER BY priority DESC
'''


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
#wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-0000[0-9]-of-00500.csv.gz")

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

jobTaskCount = sqlContext.sql(SELECT_JOB_TASK_COUNT)

print("Writing to the csv file...")

writeToCSV(jobTaskCount,"job-task-count.csv")


print("End of write.")

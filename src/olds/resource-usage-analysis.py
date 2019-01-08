import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))

SELECT_DISTINCT_TASK_EVENT = '''
SELECT 
    job_id,
    task_index,
    MAX(priority) as priority,
    MAX(cpu_request) as cpu_request,
    MAX(memory_request) as memory_request
FROM task_events
WHERE event_type = 1
GROUP BY job_id,task_index
'''



SELECT_TASK_USAGE = '''
SELECT
    job_id,
    task_index,
    MAX(maximum_memory_usage) as maximum_memory_usage,
    MAX(maximum_CPU_rate) as maximum_CPU_rate,
    MAX(assigned_memory_usage) as assigned_memory_usage
FROM task_usage
GROUP BY job_id, task_index
'''

#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[16]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
wholeFile = sc.textFile("../clusterdata-2011-2/task_usage/part-00001-of-00500.csv.gz")

#wholeFile = sc.textFile("../clusterdata-2011-2/task_events")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
taskUsage = tokens.map(lambda p: Row( 
                        
                        start_time=int(p[0]),
                        end_time=int(p[1]),
                        job_id=int(p[2]),
                        task_index=int(p[3]),
                        machine_id=int(p[4]),
    
                        cpu_rate=float(p[5] or 0 ),
    
                        canonical_memory_usage=float(p[6] or 0 ),
                        assigned_memory_usage=float(p[7] or 0 ),
                        unmapped_page_cache=float(p[8] or 0 ),
                        total_page_cache=float(p[9] or 0 ),
                        maximum_memory_usage=float(p[10] or 0 ),
                        disk_io_time=float(p[11] or 0 ),
                        local_disk_space_usage=float(p[12] or 0 ),
                        maximum_CPU_rate=float(p[13] or 0 ),
                        maximum_disk_IO_time=float(p[14] or 0 ),
                        cycles_per_instruction=float(p[15] or 0 ),
                        memory_accesses_per_instruction=float(p[16] or 0 ),
                        sample_portion=float(p[17] or 0 ),
                        aggregation_type=bool(p[18] or 0 ),
                        sampled_CPU_usage=float(p[19] or 0 )
))

# Infer the schema, and register the macine_event as a table.
schemaTaskUsage = sqlContext.createDataFrame(taskUsage)
schemaTaskUsage.registerTempTable("task_usage")



############################################################################

wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-00001-of-00500.csv.gz")

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

################################################################################

taskUsage = sqlContext.sql(SELECT_TASK_USAGE)
taskEvents = sqlContext.sql(SELECT_DISTINCT_TASK_EVENT)

joinExpr = [taskUsage["job_id"] == taskEvents["job_id"], taskUsage["task_index"] == taskEvents["task_index"]]
joinResult = taskUsage.join(taskEvents,joinExpr).orderBy(taskUsage["job_id"], taskUsage["task_index"]).drop(taskEvents["job_id"]).drop(taskEvents["task_index"]).cache()

joinResult.show();

#print("Writing to the csv file...")
#writeToCSV(jobTaskCount,"job-task-count.csv")
#print("End of write.")

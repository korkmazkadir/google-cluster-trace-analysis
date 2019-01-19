import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time
from pyspark.sql.types import *

# def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
    dataFrame.repartition(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save(
        "../output/")



SELECT_REQUESTED_RESOURCE = '''
SELECT 
    job_id, 
    task_index, 
    cpu_request, 
    memory_request
FROM task_events
'''



SELECT_MAXIMUM_RESOURCE_USE = '''
SELECT
    job_id,
    task_index,
    max(maximum_CPU_rate) as maximum_CPU_usage,
    max(maximum_memory_usage) as maximum_memory_usage
FROM task_usage
GROUP BY job_id, task_index
'''


SELECT_MACHINE_UTILIZATION = '''
SELECT
    machine_id,
    sum(((end_time - start_time)/1000000)* assigned_memory_usage * sampled_CPU_usage) as machine_utilization
FROM task_usage
GROUP BY machine_id
'''


SELECT_RESOURCE_REQUESTED_USED = '''
SELECT
    mru.job_id,
    mru.task_index,
    rr.cpu_request,
    rr.memory_request,
    mru.maximum_CPU_usage,
    mru.maximum_memory_usage,
    round(mru.maximum_CPU_usage * 100 / rr.cpu_request,1) as used_cpu_percent,
    round(mru.maximum_memory_usage * 100 / rr.memory_request,1) as used_memory_percent
    
FROM maximum_resource_use mru
INNER JOIN resource_request rr 
    on rr.job_id = mru.job_id and rr.task_index = mru.task_index
WHERE rr.cpu_request > mru.maximum_CPU_usage and rr.memory_request > mru.maximum_memory_usage
'''


#### Driver program

import pyspark
conf = pyspark.SparkConf()
conf.set("spark.driver.memory", "11g")
conf.setMaster("local[*]")

# start spark with 1 worker thread
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
#wholeFile = sc.textFile("../clusterdata-2011-2/task_usage")


#wholeFile = sc.textFile("../clusterdata-2011-2/task_usage/part-00001-of-00500.csv.gz")


############################################################################

#wholeFile = sc.textFile("../clusterdata-2011-2/task_events/part-00497-of-00500.csv.gz")

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

################################################################################


wholeFile = sc.textFile("../clusterdata-2011-2/task_usage")

# split each line into an array of items
tokens = wholeFile.map(lambda x: x.split(','))

# creates columns
taskUsage = tokens.map(lambda p: Row(

    start_time=int(p[0]),
    end_time=int(p[1]),
    job_id=int(p[2]),
    task_index=int(p[3]),
    machine_id=int(p[4]),

    cpu_rate=float(p[5] or 0),

    canonical_memory_usage=float(p[6] or 0),
    assigned_memory_usage=float(p[7] or 0),
    unmapped_page_cache=float(p[8] or 0),
    total_page_cache=float(p[9] or 0),
    maximum_memory_usage=float(p[10] or 0),
    disk_io_time=float(p[11] or 0),
    local_disk_space_usage=float(p[12] or 0),
    maximum_CPU_rate=float(p[13] or 0),
    maximum_disk_IO_time=float(p[14] or 0),
    cycles_per_instruction=float(p[15] or 0),
    memory_accesses_per_instruction=float(p[16] or 0),
    sample_portion=float(p[17] or 0),
    aggregation_type=bool(p[18] or 0),
    sampled_CPU_usage=float(p[19] or 0)
))

# Infer the schema, and register the macine_event as a table.
schemaTaskUsage = sqlContext.createDataFrame(taskUsage)
schemaTaskUsage.registerTempTable("task_usage")



taskRequestedMemoryCPU = sqlContext.sql(SELECT_REQUESTED_RESOURCE).cache()
taskRequestedMemoryCPU.registerTempTable("resource_request")


taskMaxMemoryCPU = sqlContext.sql(SELECT_MAXIMUM_RESOURCE_USE)
taskMaxMemoryCPU.registerTempTable("maximum_resource_use")

buckets = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]

joinedData = sqlContext.sql(SELECT_RESOURCE_REQUESTED_USED).cache()
bins, cpu_percent = joinedData.select("used_cpu_percent").rdd.flatMap(lambda x: x).histogram(buckets)
bins, memory_percent = joinedData.select("used_memory_percent").rdd.flatMap(lambda x: x).histogram(buckets)



machineUtilization = sqlContext.sql(SELECT_MACHINE_UTILIZATION)


print(bins)
print(cpu_percent)
print("###############################################")
print(memory_percent)

print("Writing to the csv file...")

writeToCSV(machineUtilization,"machine-utilization.csv")

print("End of write.")

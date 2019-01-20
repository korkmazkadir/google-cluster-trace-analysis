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


SELECT_JOINED_MACHINE_UTILIZATION = '''
SELECT
  mu.machine_id, 
  dme.cpu,
  dme.memory,
  mu.utilization,
  (29 * 60 * 60 * cpu * memory) as available_capacity_for_29_days
FROM macine_utilization mu
LEFT JOIN distinct_machine_events dme on dme.machine_id = mu.machine_id
ORDER BY mu.utilization
'''


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

##########################################################################################################
# read the input file into an RDD[String]
#wholeFile = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")
wholeFile = sc.textFile("../clusterdata-2011-2/machine_events")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
machineEvents = tokens.map(lambda p: Row( time=int(p[0]), machine_id=int(p[1]), event_type=int(p[2]), platform_id=p[3], cpu=float(p[4] or 0) , memory=float(p[5] or 0 ) ))

# Infer the schema, and register the macine_event as a table.
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)
schemaMachineEvents.registerTempTable("macine_events")
##########################################################################################################
# read the input file into an RDD[String]
wholeFile = sc.textFile("./machine-utilization.csv")

# split each line into an array of items
tokens = wholeFile.map(lambda x : x.split(','))

# creates columns
machineUtilization = tokens.map(lambda p: Row( machine_id=int(p[0]), utilization=float(p[1] or 0) ))

# Infer the schema, and register the macine_event as a table.
schemaMachineUtilization = sqlContext.createDataFrame(machineUtilization)
schemaMachineUtilization.registerTempTable("macine_utilization")
##########################################################################################################




distinctMachineEvents = sqlContext.sql(SELECT_DISTINCT_MACHINE_EVENTS)
distinctMachineEvents.registerTempTable("distinct_machine_events")


machineUtilizationJoined = sqlContext.sql(SELECT_JOINED_MACHINE_UTILIZATION).cache()



count = machineUtilizationJoined.count()
print("Distinct machine event count : {}".format(count))

machineUtilizationJoined.show(100,False)

print("Writing to the csv file...")

writeToCSV(machineUtilizationJoined,"machine-utilization.csv")

print("End of write.")

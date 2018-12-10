import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time
from pyspark.sql.functions import desc, expr, min

#def toCSVLine(data):
#  return ','.join(str(d) for d in data)

def writeToCSV(dataFrame, fileName):
  dataFrame.coalesce(1).write.mode('append').format('com.databricks.spark.csv').options(header='true').save("../output/".format(fileName))


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

# Infer the schema, and register the macine_event as a table.
schemaMachineEvents = sqlContext.createDataFrame(machineEvents)


schemaMachineEvents.selectExpr("count( distinct machine_id)").show()


t = schemaMachineEvents.groupBy("event_type").count()

print("T count {0}".format(t.count()))

t.show();


p = schemaMachineEvents.select("*").where("event_type == 0").groupBy("machine_id").agg( min("time") )
p.show()
print("P count {0}".format(p.count()))

distinctMachineEvents = schemaMachineEvents.select("*").where("event_type == 0").select(
    "machine_id",
    "platform_id",
    "cpu", 
    "memory").distinct().selectExpr("*","(cpu + memory)/2 as capacity").cache()


test = distinctMachineEvents.groupBy("machine_id").count().where("count > 1")
count = test.count()
print("Count {0}".format(count))
test.show(count)


joinExpr = test["machine_id"] == distinctMachineEvents["machine_id"]
joinResult = schemaMachineEvents.join(test,joinExpr).orderBy(distinctMachineEvents["machine_id"]).cache()
print("Join result count {0}".format(joinResult.count()))
joinResult.show(joinResult.count())


machineDistAccordingToCPU = distinctMachineEvents.groupBy("cpu").count().orderBy(desc("cpu")).show()
machineDistAccordingToMemory = distinctMachineEvents.groupBy("memory").count().orderBy(desc("memory")).show()
machineDistAccordingToCapacity = distinctMachineEvents.groupBy("capacity").count().orderBy(desc("capacity"))
machineDistAccordingToPlatform = distinctMachineEvents.groupBy("platform_id").count().orderBy(desc("platform_id")).show()

machineDistAccordingToCapacity.selectExpr("sum(count) as machine_count").show()

machineDistAccordingToCapacity.show()

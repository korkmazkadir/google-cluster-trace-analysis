import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time



#### Driver program

# start spark with 1 worker thread

SparkContext.setSystemProperty('spark.executor.memory', '14g')
SparkContext.setSystemProperty('spark.driver.memory', '14g')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '3g')


sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")

#creates sql context
sqlContext = SQLContext(sc)

# read the input file into an RDD[String]
wholeFile = sc.textFile("../../clusterdata-2011-2/task_events/part-000[0-9][0-9]-of-00500.csv.gz")

#wholeFile = sc.textFile("../../clusterdata-2011-2/task_events")

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


#taskEventsData = schemaTaskEvents.select("cpu_request", "memory_request").cache()

#print("Row count count {}".format(taskEventsData.count()))
#print("Distinct count {}".format(taskEventsData.distinct().count()))

#Row count:  70651601
taskEventsSubSet = schemaTaskEvents.select("job_id", "task_index", "machine_id", "priority", "cpu_request", "memory_request").distinct().cache()
print("Row count count {}".format(taskEventsSubSet.count()))

task_dictionary = taskEventsSubSet.rdd.map(lambda taskEvent: ( ( taskEvent.job_id,  taskEvent.task_index ), [taskEvent.machine_id, taskEvent.priority, taskEvent.cpu_request,taskEvent.memory_request] )).collectAsMap()
#task_dictionary = taskEventsSubSet.rdd.map(lambda taskEvent: ( ( taskEvent.job_id,  taskEvent.task_index ), [taskEvent.machine_id] )).collectAsMap()


print("Size of dictionary : " + str(sys.getsizeof(task_dictionary)))

print("Length of dictionary {}".format(len(task_dictionary)))

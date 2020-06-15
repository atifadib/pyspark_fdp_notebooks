from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def save_rdd(rdd):
    if not rdd.isEmpty():
        rdd.toDF(["state","count"]).write.csv("my_state_counter.csv",mode="overwrite")

sc = SparkContext()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
ssc.checkpoint('./my_ckpt/')

data = ssc.textFileStream('./my_logs')

lines = data.map(lambda x: x.split(","))

states = lines.map(lambda x: (x[0],int(x[1])))

#states = states.reduceByKey(lambda x,y: x+y)
#states = states.groupBy(lambda x: x[0]).count()
states = states.updateStateByKey(updateFunction)

#states.pprint(num=60)
#states.coalesce(1).write.csv("state_data.csv")
states.foreachRDD(save_rdd)
ssc.start()
ssc.awaitTermination()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

sc = SparkContext()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
ssc.checkpoint('./my_ckpt/')

data = ssc.textFileStream('./my_logs')

lines = data.map(lambda x: x.split("\t"))

states = lines.map(lambda x: (x[6],1))

#states = states.reduceByKey(lambda x,y: x+y)
#states = states.groupBy(lambda x: x[0]).count()
states = states.updateStateByKey(updateFunction)

states.pprint(num=60)
ssc.start()
ssc.awaitTermination()



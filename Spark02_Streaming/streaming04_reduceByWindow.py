import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    batchDuration = 2
    windowDuration = 10
    slideDuration = 2
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    sc = SparkContext(appName="StreamingWindowCount")
    ssc = StreamingContext(sparkContext=sc, batchDuration=batchDuration)
    ssc.checkpoint("file///tmp/spark")
    lines = ssc.socketTextStream(hostname=hostname, port=port)
    _sum = lines.reduceByWindow(
        lambda x, y: int(x) + int(y),
        lambda x, y: int(x) - int(y),
        windowDuration,
        slideDuration)
    _sum.pprint()
    ssc.start()
    ssc.awaitTermination()

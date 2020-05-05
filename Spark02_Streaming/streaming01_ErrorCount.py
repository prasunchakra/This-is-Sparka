import sys
from pyspark import SparkContext
# The SparkContext sets up internal services and a connection to spark from our application
from pyspark.streaming import StreamingContext

# The StreamingContext is the entry point for spark streaming functionality

if __name__ == "__main__":
    batchInterval = int(sys.argv[1])
    hostname = sys.argv[2]
    port = int(sys.argv[3])
    sc = SparkContext(appName="streamingErrorCount")
    ssc = StreamingContext(sc, batchInterval)
    ssc.checkpoint("file:///tmp/spark/")  # setup fault tolerance of the created streams, Backup data
    lines = ssc.socketTextStream(hostname, port)
    # Here lines is a sequence of RDDs, the DStream
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda word: "Error:" in word) \
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()  # Start listening to streaming data
    ssc.awaitTermination()  # Application waits forever for streaming data until explicitly terminated

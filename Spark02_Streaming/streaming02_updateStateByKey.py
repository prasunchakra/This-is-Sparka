import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def count_words(new_values, last_sum):
    if last_sum is None:
        last_sum = 0
    return sum(new_values, last_sum)


if __name__ == "__main__":
    batchInterval = int(sys.argv[1])
    hostname = sys.argv[2]
    port = int(sys.argv[3])
    sc = SparkContext(appName="streamingErrorCount")
    ssc = StreamingContext(sc, batchInterval)
    ssc.checkpoint("file///tmp/spark")
    lines = ssc.socketTextStream(hostname, port)

    # key=word sum all values associated with the same key works across all entities in the stream
    wordCounts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(count_words)

    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()


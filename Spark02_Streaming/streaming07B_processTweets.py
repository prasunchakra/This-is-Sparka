from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def count_words(new_value, last_sum):
    if last_sum is None:
        last_sum = 0
    return sum(new_value, last_sum)


if __name__ == "__main__":
    batchDuration = 2
    hostname = "localhost"
    port = 7777
    sc = SparkContext(appName="StreamingWindowCount")
    ssc = StreamingContext(sparkContext=sc, batchDuration=batchDuration)
    ssc.checkpoint("file///tmp/spark")
    lines = ssc.socketTextStream(hostname=hostname, port=port)

    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda w: w.startswith("#")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(count_words)

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()

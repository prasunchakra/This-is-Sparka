import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def count_words(new_value, last_sum):
    if last_sum is None:
        last_sum = 0
    return sum(new_value, last_sum)


def create_context(_hostname, _port):
    batch_duration = 2
    sc = SparkContext(appName="StreamingWindowCount")
    _ssc = StreamingContext(sparkContext=sc, batchDuration=batch_duration)
    _ssc.checkpoint("file///tmp/spark")
    lines = _ssc.socketTextStream(hostname=_hostname, port=_port)

    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda w: w.startswith("#")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(count_words)

    word_counts.pprint()

    return _ssc  # Return the streaming context


if __name__ == "__main__":

    # hostname, port, checkpoint_dir = sys.argv[1:]
    hostname = "localhost"
    port = 7777
    checkpoint_dir = "file///tmp/spark"
    ssc = StreamingContext.getOrCreate(checkpoint_dir, lambda: create_context(hostname, port))
    ssc.start()
    ssc.awaitTermination()

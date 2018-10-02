import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# netcat和spark streaming
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: spark0901.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="spark0901")
    ssc = StreamingContext(sc, 5)

    # TODO: ... 根据业务需求

    # Define the input sources by creating input DStreams.
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # Define the streaming computations by applying transformation
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    # output operations to DStreams
    counts.pprint()

    # Start receiving data and processing it
    ssc.start()
    # Wait for the processing to be stopped
    ssc.awaitTermination()

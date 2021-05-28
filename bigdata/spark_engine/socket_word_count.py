"""
https://blog.csdn.net/u010520724/article/details/116490946
https://github.com/apache/spark/tree/v3.1.2-rc1/examples/src/main/python/streaming
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == '__main__':
    # Create a local StreamingContext with two working thread and batch interval of 10 second
    sc = SparkContext("local[2]", "NetworkWordCount")

    ssc = StreamingContext(sc, 10)
    
    # Create DStream that will connect to the stream of input lines from connection to localhost:9999
    # lines is DStream representing the data stream extracted via the ssc.socketTextStream.
    # `nc -lk 9999` to send data
    lines = ssc.socketTextStream("localhost", 9999)

    # Split lines into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    # Start the computation
    ssc.start()

    # Wait for the computation to terminate
    ssc.awaitTermination()
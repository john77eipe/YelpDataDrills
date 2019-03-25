import os

from operator import add
from pyspark import SparkContext

os.environ["SPARK_HOME"] = "/usr/local/spark"


if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("/Users/johne/Desktop/saved", 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    sc.stop()
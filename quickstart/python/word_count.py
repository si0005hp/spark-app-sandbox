from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

lines = spark.read.text("/opt/spark/README.md").rdd.map(lambda r: r[0])
counts = (
    lines.flatMap(lambda x: x.split(" "))
    .map(lambda x: (x, 1))
    .reduceByKey(add)
)

counts.saveAsTextFile("/tmp/word_counts")

output = counts.collect()
for word, count in output:
    print("%s: %i" % (word, count))

spark.stop()

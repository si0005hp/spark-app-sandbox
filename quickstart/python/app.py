from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text("/opt/spark/README.md").cache()

numAs = logData.filter(logData.value.contains("a")).count()
numBs = logData.filter(logData.value.contains("b")).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()

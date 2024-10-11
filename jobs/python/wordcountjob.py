from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = """
INFO 2024-06-01 10:00:00 User1 Login successful
ERROR 2024-06-01 10:01:00 User2 Login failed
INFO 2024-06-01 10:02:00 User1 Viewed profile
INFO 2024-06-01 10:03:00 User3 Login successful
WARNING 2024-06-01 10:04:00 User2 Multiple failed login attempts
ERROR 2024-06-01 10:05:00 User2 Account locked
INFO 2024-06-01 10:06:00 User4 Login successful
"""

words = spark.sparkContext.parallelize(text.split())

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()

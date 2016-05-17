from pyspark import SparkContext

sc = SparkContext(appName = "basic")

testRDD = sc.textFile("test.txt")
def addOne(n):
    return n+1
result = testRDD.map(lambda e: long(e)).map(addOne)
result.saveAsTextFile("result")
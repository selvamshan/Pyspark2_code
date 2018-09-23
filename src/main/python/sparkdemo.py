from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("spark demo").setMaster("local")
sc = SparkContext(conf=conf)
rdd = sc.parallelize([1,2,3,4,5])
print(rdd.collect())

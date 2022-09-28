from pyspark import SparkContext, SparkConf
import math

#setup
appName="appName"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def printrdd(rdd):
  for row in rdd.collect():
    print(row)

# lines are word <space> url
hits = sc.textFile('output/hits')
thing = hits.map(lambda x: (x, 1))
sums = thing.reduceByKey(lambda x, y: x + y)
tfs = sums.map(lambda x: (x[0], math.log(1 + x[1])))
formatted = tfs.map(lambda tup: " ".join([str(x) for x in tup]))
formatted.coalesce(1).saveAsTextFile("output/tfs")

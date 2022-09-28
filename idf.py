from pyspark import SparkContext, SparkConf
import math

#NUMBER\OF\CRALED\DOCUMENTS
n = 5000

#setup
appName="appName"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def printrdd(rdd):
  for row in rdd.collect():
    print(row)

# lines are word <space> url <space> tf
tfs= sc.textFile('output/tfs')
thing = tfs.map(lambda x: (x.split(' ')[0], 1))
sums = thing.reduceByKey(lambda x, y: x + y)
idfs = sums.map(lambda x: (x[0], math.log(n / (1 + x[1]) + 1)))
formatted = idfs.map(lambda tup: " ".join([str(x) for x in tup]))
formatted.coalesce(1).saveAsTextFile("output/idfs")

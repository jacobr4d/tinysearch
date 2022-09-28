from pyspark import SparkContext, SparkConf

#setup
appName="appName"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def printrdd(rdd):
  for row in rdd.collect():
    print(row)

# lines are fromURL <space> toURL, read them and remove duplicates
lines = sc.textFile('output/links')
lines = lines.distinct()

# all links
fromTo = lines.map(lambda x: (x.split(' ')[0], x.split(' ')[1]))
toFrom = fromTo.map(lambda x: (x[1], x[0]))
outList = fromTo.groupByKey()
inList = toFrom.groupByKey()
outDegree = fromTo.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
inDegree = toFrom.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

# sets of urls
urls = fromTo.flatMap(lambda x: [x[0], x[1]]).distinct()
n = urls.count()
froms = fromTo.map(lambda x: x[0]).distinct()
tos = fromTo.map(lambda x: x[1]).distinct()
sinks = urls.subtract(froms)

print("urls, froms, tos, sinks")
print(urls.count(), froms.count(), tos.count(), sinks.count())

# init (url, rank)
l = 0.8
rank = urls.map(lambda x: (x, 1.0 / n)) 

for i in range(5):
  inRankNonSinks = outList.join(rank).flatMap(lambda x: [(i, float (x[1][1])/len(x[1][0])) for i in x[1][0]]).reduceByKey(lambda x, y: x + y)
  inRankAll = rank.leftOuterJoin(inRankNonSinks).map(lambda x: (x[0], x[1][1] if x[1][1] != None else 0))
  sinkRank = sinks.map(lambda x: (x, "dummy")).join(rank).map(lambda x: x[1][1]).reduce(lambda x, y: x + y)
  rank = inRankAll.map(lambda x: (x[0], (1 - l + l * sinkRank / n + l * x[1])))

rank = rank.map(lambda line: " ".join([str(x) for x in line]))
rank.coalesce(1).saveAsTextFile("output/prs")

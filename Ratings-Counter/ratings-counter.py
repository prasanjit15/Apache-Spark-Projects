from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

line = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedresults = collections.OrderedDict(sorted(result.items()))

for key, value in sortedresults.iteritems():
	print "%s %i" % (key, value)



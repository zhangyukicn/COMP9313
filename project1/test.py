#set configuration properties by passing a SparkConf object to SparkContext
from pyspark import SparkContext, SparkConf
#pyspark.SparkConf(loadDefaults=True, _jvm=None, _jconf=None).
# Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
#which will load values from spark.* Java system properties as well.
conf = SparkConf().setMaster("local").setAppName("example")
#setMaster(value),Set master URL to connect to.
#setAppName(value),Set application name.
#setAppName(value)
#class pyspark.SparkContext(master=None, appName=None, sparkHome=None, pyFiles=None, environment=None, batchSize=0,
#serializer=PickleSerializer(), conf=None, gateway=None, jsc=None, profiler_cls=<class 'pyspark.profiler.BasicProfiler'>

sc = SparkContext(conf=conf)
data = range(1,100)
#parallelize(c, numSlices=None)
#Distribute a local Python collection to form an RDD. Using xrange is recommended if the input represents a range
# for performance.


rdd = sc.parallelize(data)
#collect():Return a list that contains all of the elements in this RDD.
print(rdd)
rdd.collect()
#print(rdd.collect())


rdd = sc.parallelize(data,5)
#glom():Return an RDD created by coalescing合并 all elements within each partition into a list.
rdd.glom().collect()
print(rdd.glom().collect())



rdd1 = sc.textFile('example.txt')
rdd1.collect()
print(rdd1.collect())

#narrow transformation
#map(f, preservesPartitioning=False),Return a new RDD by applying a function to each element of this RDD.
#lambda arguments : expression
rdd_map =rdd1.map(lambda x:(x,1))
rdd_map.collect()
print('111')
print(rdd_map.collect())
#take(num):Take the first num elements of the RDD.
rdd_map.take(4)
print(rdd_map.take(4))


text = ['word count','word word count']
rdd2 = sc.parallelize(text)
rdd2.collect()
rdd2.map(lambda x: x.split()).collect()
print(rdd2.map(lambda x: x.split()).collect())


#flatMap(f, preservesPartitioning=False)：Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
rdd_flatmap = rdd2.flatMap(lambda x:x.split())
rdd_flatmap.collect()
print(rdd_flatmap.collect())

#filter(f):Return a new RDD containing only the elements that satisfy a predicate.
#rdd_flatmap = rdd2.flatMap(lambda x:x.split()).filter(lambda x: x!='count')
rdd_filter = rdd_flatmap.filter(lambda x:x!='count')
print(rdd_filter.collect())

#wide transformation
#reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash>) :
#Merge the values for each key using an associative and commutative reduce function.
print("12345")
rdd_reducebykey = rdd_map.reduceByKey(lambda x,y:x+y)
rdd_reducebykey.collect()
print(rdd_reducebykey.collect())

#sortByKey(ascending=True, numPartitions=None, keyfunc=<function RDD.<lambda>>)
#Sorts this RDD, which is assumed to consist of (key, value) pairs.
rdd_flatmap.sortByKey().collect()
print("rdd_flatmap.sortByKey().collect():",rdd_flatmap.sortByKey().collect())

#groupByKey(numPartitions=None, partitionFunc=<function portable_hash>)
#Group the values for each key in the RDD into a single sequence.
#Hash-partitions the resulting RDD with numPartitions partitions.
rdd_group = rdd_map.groupByKey()
rdd_group.collect()
print(rdd_group.collect())

rdd_group = rdd_map.groupByKey()
for i in rdd_group.collect():
    print(i[0],[j for j in i[1]])



def op(x,y):
    return (x+y)

rdd_reducebykey = rdd_map.reduceByKey(op)
rdd_reducebykey.collect()
print("rdd_reducebykey.collect()",rdd_reducebykey.collect())


#reduce(f):
# Reduces the elements of this RDD using the specified commutative and associative binary operator.
# Currently reduces partitions locally
rdd.reduce(lambda x,y:x+y)
print(rdd.reduce(lambda x,y:x+y))

rdd.count()
print(rdd.count())





#test
raw_data =[("j","M",83),("j","A",100),("P","A",20),("P","M",90)]
rdd = sc.parallelize(raw_data)
print(rdd.collect())

p = rdd.map(lambda x:(x[0],(x[2],1)))
print(rdd.map(lambda x:(x[0],(x[2],1))).collect())
f = rdd.map(lambda x:(x[0],(x[2],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
print(p.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).collect())
z = rdd.map(lambda x:(x[0],(x[2],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
print(z.collect())



'''
from pyspark import SparkContext, SparkConf

def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf = conf)
    return sc

sc = createSC()
'''
'''
raw_data = [("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78), ("Tina", "Physics", 73),
    ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93),
    ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74)]

'''
'''
rdd = sc.parallelize(raw_data)
rdd_1 = sc.parallelize(raw_data)
rdd_2 = rdd_1.map(lambda x:(x[0], x[2]))
rdd_3 = rdd_2.reduceByKey(lambda x, y:max(x, y))
rdd_4 = rdd_2.reduceByKey(lambda x, y:min(x, y))
rdd_5 = rdd_3.join(rdd_4)
rdd_6 = rdd_5.map(lambda x: (x[0], x[1][0]+x[1][1]))
rdd_6.collect()

print(rdd_6.collect())
'''
'''
rdd = sc.parallelize(raw_data)
rdd_1 = sc.parallelize(raw_data)
rdd_2 = rdd_1.map(lambda x:(x[0], x[2]))
print("rdd2",rdd_2.collect())

def createCombiner(value):
    return [value]

def mergeValue(acc,value):
    return acc + [value]

def mergeCombiners(acc1,acc2):
    return (acc1+acc2)

rdd_3 = rdd_2.combineByKey(createCombiner,mergeValue,mergeCombiners)
print(rdd_3.collect())

rdd_4 = rdd_3.map(lambda x:(x[0],min(x[1])+max(x[1])))
print(rdd_4.collect())

'''
'''
import math
a = math.acos(0.8)
b = math.degrees(a)
c = 1-(b/180)
print(b)


N = 500
R = 20000000
K = 200
B = (R/N)
list1 = [[0,0,0,0,0]]
for i in range(1,K+1):
    lista = [0,0,0,0,0]
    lista[4] = list1[-1][3] /(N-i+1) + list1[-1][4]
    lista[3] = 2 * list1[-1][2] /(N-i+1) + list1[-1][3] - list1[-1][3]/(N-i+1)
    lista[2] = 3 * list1[-1][1]/(N-i+1) + list1[-1][2] - 2*list1[-1][2]/(N-i+1)
    lista[1] = 4 * list1[-1][0]/(N-i+1) + list1[-1][1] - 3*list1[-1][1]/(N-i+1)
    lista[0] = i * R/N - 2*lista[1] -3*lista[2] -4*lista[3]-5*lista[4]
    list1.append(lista)

print(list1[200][4])
'''
'''
table = [['1' for _ in range(201)] for _ in range(6)]
print(len(table),len(table[0]))

table[1][1] =B
for i in range(2,6):
    table[i][1] =0

for i in table:
    print(i)

def updatetable(l,k,table):
    if l ==1:
        table[l][k]=k*B - 2*table[2][k] - 3*table[3][k] -4*table[4][k] -5*table[5][k]
        return
    table[l][k] = int((6-l)*table[l-1][k-1])/int(N-k+1) +table[l][k-1] - int((6-l-1)*table[l][k-1])/int(N-k+1)
   # print(table[l][k])

for k in range(2,201):
    for l in [5,4,3,2,1]:
        updatetable(l,k,table)

print(table[5][200])
'''

raw_data = [("Joseph", 83),
            ("Joseph",  74),
            ("Joseph", 91),
            ("Jimmy", 69),
            ("Jimmy", 62),
            ("Jimmy",  97),
            ("Jimmy",80),
            ("Tina", 78),
            ("Tina",  73),
            ("Tina",  68),
            ("Tina",  87),
            ("Thomas",  87),
            ("Thomas", 91),
            ("Thomas", 74),
            ("Armin",50)
            ]
'''
rdd_1 = sc.parallelize(raw_data)

def createCombiner(value):
    return tuple([value])

def mergeValue(acc,value):
    y = acc + tuple([value])
    return tuple(y)

def mergeCombiners(acc1,acc2):
    print(tuple(acc1) + tuple(acc2))
    y = sorted(tuple(acc1)+ tuple(acc2),reverse= True)[:2]
    return tuple(y)

rdd_2 = rdd_1.combineByKey(createCombiner,mergeValue,mergeCombiners)

print(rdd_2.collect())
'''
'''
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark=SparkSession \
.builder \
.appName('my_app_name') \
.getOrCreate()
import pyspark.sql
df = spark.createDataFrame([(1,9313,80),
     (1,9318,75),
     (1,6714,70),
     (2,9021,70),
     (2,9311,70),
     (3,9313,90),
     (3,9020,50)],['ID','course_id', 'grade'])

df.show()
def1 = df.groupBy('ID').max('grade')
def2 = df.groupBy('ID').min('grade')
def3 = def1.join(def2,'ID')

def4 = def3.sort('ID').withColumnRenamed('max(grade)','max')
def5 = def4.sort('ID').withColumnRenamed('min(grade)','min')
def5.show()
'''

A=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26]
B=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']

def function(words):
    word_number =[]
    number = 0
    for i in words:
        number = A[B.index(i)] - 1
        word_number.append(number)
    sum_number = sum(word_number)
    dive_number =  sum_number% 8
    return dive_number

def function_2(words):
    length = len(words)
    dive_number = length % 8
    return dive_number

print(function('spark'),function('map'),function('reduce'))
print(function_2('spark'),function_2('map'),function_2('reduce'))





import math

number = (1-(math.e)**((-2*8)/5))**2

print(number)


from scipy import spatial

dataSetI = [-1/3,5/3,0,0,-4/3]
dataSetII = [1/3,0,4/3,-5/3,0]
result = 1 - spatial.distance.cosine(dataSetI, dataSetII)
print(result)




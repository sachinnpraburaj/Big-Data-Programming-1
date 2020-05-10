from pyspark import SparkConf, SparkContext
import sys, operator
from pyspark.sql import SparkSession,functions as fu, types
assert sys.version_info >= (3, 5)

def split_node_edge(text):
    split_dat = text.split(":")
    if len(split_dat[1]) > 0:
        edges = split_dat[1].split(" ")
        for i in edges:
            if len(i) > 0:
                yield (int(split_dat[0]),int(i))

def reorder(x):
    node = x[0]
    dest = x[1][0]
    source = x[1][1][0]
    dist = x[1][1][1]
    return (dest,(node,dist+1))

def min_dist(x1, x2):
        if x1[1] <= x2[1]:
                return x1
        else:
                return x2

def returnfinalpath(source,dest,path):
    toRet = [dest]
    while dest != source and dest != '-':
        loc = path.lookup(dest)
        dest = loc[0][0]
        if dest != '':
            toRet.append(dest)
    return toRet

def main(inputs,output,source,dest):
    text = sc.textFile(inputs+"/links-simple-sorted.txt")
    node_edge = text.flatMap(split_node_edge).cache()
    path = sc.parallelize([(source,('-',0))])

    for i in range(6):
        joined = node_edge.join(path.filter(lambda x: x[1][1] == i-1))
        convert = joined.map(reorder)
        path = path.union(convert).reduceByKey(min_dist)
        path.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
    try:
        toRetPath = returnfinalpath(source,dest,path)
        toOut = sc.parallelize(toRetPath[::-1])
        toOut.coalesce(1).saveAsTextFile(output+'/path')
    except:
        print ("*******************\nNo path exists between the nodes!!\n*******************")



if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest path')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    dest = int(sys.argv[4])
    main(inputs,output,source,dest)

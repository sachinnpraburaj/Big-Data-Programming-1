from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


#functions
def ret_keyval(dict):
    return (dict['subreddit'],(1,dict['score']))

def add_pairs(tup1,tup2):
    return (tup1[0]+tup2[0],tup1[1]+tup2[1])

def find_avg(tup):
    return (tup[0],tup[1][1]/tup[1][0])

def main(inputs, output):
    text = sc.textFile(inputs)
    o1 = text.map(json.loads).map(ret_keyval)
    o2 = o1.reduceByKey(add_pairs).map(find_avg)
    o3 = o2.sortByKey().map(json.dumps).coalesce(1)
    o3.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

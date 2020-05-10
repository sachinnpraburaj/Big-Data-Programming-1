from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


#functions
def ret_keyval(dict):
    return (dict['subreddit'],(1,dict['score']))

def add_pairs(pair1,pair2):
    return (pair1[0]+pair2[0],pair1[1]+pair2[1])

def find_avg(tup):
    return (tup[0],tup[1][1]/tup[1][0])

def bcastfunc(json_dict,averages):
    return (json_dict['score']/averages.value[json_dict['subreddit']],json_dict['author'])

def main(inputs, output):
    text = sc.textFile(inputs)
    json_dict = text.map(json.loads).cache()

    # for finding average
    key_val = json_dict.map(ret_keyval)
    sub_average = key_val.reduceByKey(add_pairs).map(find_avg).filter(lambda x: x[1]>0)
    to_broadcast = dict(sub_average.collect())
    averages = sc.broadcast(to_broadcast)

    # for finding best comment
    new_key_val = json_dict.map(lambda x: bcastfunc(x,averages))
    new_key_val.sortBy(lambda x : x[0],False).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

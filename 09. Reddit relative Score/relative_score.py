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

def main(inputs, output):
    text = sc.textFile(inputs)
    json_dict = text.map(json.loads).cache()

    # for finding average
    key_val = json_dict.map(ret_keyval)
    sub_average = key_val.reduceByKey(add_pairs).map(find_avg).filter(lambda x: x[1]>0)
    sub_average.sortByKey().map(json.dumps)

    # for finding best comment
    new_key_val = json_dict.map(lambda c: (c["subreddit"], c))
    joined = new_key_val.join(sub_average)
    best_comment = joined.map(lambda x: (x[1][0]['score']/x[1][1],x[1][0]['author']))
    best_comment.sortBy(lambda x: x[0],False).saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def extract(json_dict):
    return (json_dict['subreddit'],json_dict['score'],json_dict['author'])


def main(inputs, output):
    text =  sc.textFile(inputs).cache()
    main_features = text.map(json.loads).map(extract)
    filtered = main_features.filter(lambda x: 'e' in x[0])
    filtered.filter(lambda x: x[1]>0).map(json.dumps).saveAsTextFile(output + '/positive')
    filtered.filter(lambda x: x[1]<=0).map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit ETL')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

# time taken without cache = 20.796,18.857,20.653
# time taken with cache = 14.925,15.595,15.683
# time taken with cache in wrong place = 21.783, 21.570,21.215

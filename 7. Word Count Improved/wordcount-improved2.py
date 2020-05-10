from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
assert sys.version_info >= (3, 5)

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in re.split(wordsep,line):
        yield (w.lower(), 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    text = sc.textFile(inputs)
    words = text.flatMap(words_once).filter(lambda kv:kv[0] != "")
    wordcount = words.repartition(8).reduceByKey(operator.add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

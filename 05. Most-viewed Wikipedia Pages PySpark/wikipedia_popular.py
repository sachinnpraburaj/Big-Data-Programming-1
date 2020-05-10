from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def line_sep(line):
    yield tuple(line.split())

def convert_int(feat):
    feat = list(feat) # since tuples are immutable
    feat[3] = int(feat[3])
    return tuple(feat)

def filter_rec(feat):
    if feat[2] != 'Main_Page':
        if not feat[2].startswith("Special:"):
            if feat[1] == "en":
                return True

def remove_useless(feat):
    return (feat[0],(feat[3],feat[2]))

def max_page(feat1,feat2):
    if(feat1[0]<feat2[0]):
        return feat2
    #elif(feat1[0]==feat2[0]):	#Checks if two pages have same number of views and adds the page to the tuple
    #    return feat1 + (feat2[1],)
    else:
        return feat1

def get_key(feat):
    return feat[0]

def tab_separated(feat):
    return "%s\t%s" % (feat[0], tuple(feat[1],))

text = sc.textFile(inputs)
features = text.flatMap(line_sep).map(convert_int)
filtered_features = features.filter(filter_rec).map(remove_useless)
pagecount = filtered_features.reduceByKey(max_page)
outdata = pagecount.sortBy(get_key).map(tab_separated)
features.saveAsTextFile(output)

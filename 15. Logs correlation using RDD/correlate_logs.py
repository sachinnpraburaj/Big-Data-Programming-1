from pyspark import SparkConf, SparkContext
import sys
import re
from pyspark.sql import SparkSession,functions as fu, Row, types
assert sys.version_info >= (3, 5)

def strip_regex(text):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    w = re.split(line_re,text)
    return w

def transform(line):
    return (line[1],int(line[-2]))

def corrSchema():
    wiki_schema = types.StructType([ # commented-out fields won't be read
    types.StructField('host_name', types.StringType(), True),
    types.StructField('bytes', types.LongType(), True),
    ])
    return wiki_schema

def corr_coeff(corr):
    num = (corr['sum(for_n)']*corr['sum(x*y)']) - (corr['sum(x)']*corr['sum(y)'])
    den_1 = fu.sqrt((corr['sum(for_n)']*corr['sum(x^2)']) - (corr['sum(x)']**2))
    den_2 = fu.sqrt((corr['sum(for_n)']*corr['sum(y^2)']) - (corr['sum(y)']**2))
    r = num / (den_1*den_2)
    return r

def main(inputs):
    text = sc.textFile(inputs)
    log_lines = text.map(strip_regex).filter(lambda x: len(x)==6).map(transform).map(lambda x: Row(host_name=x[0],bytes=x[1]))
    spark = SparkSession(sc)
    log_corr = spark.createDataFrame(log_lines)
    x_y = log_corr.groupBy(log_corr['host_name']).agg(fu.count(log_corr['bytes']).alias('x'),fu.sum(log_corr['bytes']).alias('y'))
    all_vals = x_y.withColumn('for_n', x_y['x']/x_y['x']).withColumn('x^2', x_y['x']**2).withColumn('y^2', x_y['y']**2).withColumn('x*y', x_y['x']*x_y['y']).drop(x_y['host_name'])
    corr = all_vals.groupBy().sum()
    r = corr.withColumn('r',corr_coeff(corr))
    r_sq = r.withColumn('r^2',r['r']**2)
    to_out = r_sq.select('r','r^2')
    toOut = list(to_out.collect()[0])
    print("r = ",toOut[0],"\nr^2 = ",toOut[1])

if __name__ == '__main__':
    conf = SparkConf().setAppName('correlate logs')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)

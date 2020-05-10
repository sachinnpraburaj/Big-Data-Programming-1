from pyspark import SparkConf, SparkContext
import sys,re
from uuid import UUID,uuid4
from pyspark.sql import SparkSession,functions as fu, Row, types
assert sys.version_info >= (3, 5)

def corr_coeff(corr):
    num = (corr['sum(for_n)']*corr['sum(x*y)']) - (corr['sum(x)']*corr['sum(y)'])
    den_1 = fu.sqrt((corr['sum(for_n)']*corr['sum(x^2)']) - (corr['sum(x)']**2))
    den_2 = fu.sqrt((corr['sum(for_n)']*corr['sum(y^2)']) - (corr['sum(y)']**2))
    r = num / (den_1*den_2)
    return r

def main(keyspace,table):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    log_corr = df.select('host','bytes')
    x_y = log_corr.groupBy(log_corr['host']).agg(fu.count(log_corr['bytes']).alias('x'),fu.sum(log_corr['bytes']).alias('y'))
    all_vals = x_y.withColumn('for_n', x_y['x']/x_y['x']).withColumn('x^2', x_y['x']**2).withColumn('y^2', x_y['y']**2).withColumn('x*y', x_y['x']*x_y['y']).drop(x_y['host'])
    corr = all_vals.groupBy().sum()
    r = corr.withColumn('r',corr_coeff(corr))
    r_sq = r.withColumn('r^2',r['r']**2)
    to_out = r_sq.select('r','r^2')
    toOut = list(to_out.collect()[0])
    print("r = ",toOut[0],"\nr^2 = ",toOut[1])


if __name__ == '__main__':
    conf = SparkConf().setAppName('spark cassandra')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(keyspace,table)

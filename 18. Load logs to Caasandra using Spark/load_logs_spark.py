from pyspark import SparkConf, SparkContext
import sys,re
from uuid import UUID,uuid4
from pyspark.sql import SparkSession,functions as fu, Row, types
assert sys.version_info >= (3, 5)

def strip_regex(text):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    w = re.split(line_re,text)
    return w

def transform(w):
    return (str(UUID(int=uuid4().int)), w[1], int(w[4]), convert(w[2]), w[3])


def convert(time):
    months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    (date,hour,min,sec)=tuple(time.split(":"))
    spt = date.split("/")
    month = months.index(spt[1])+1
    datetime = spt[2]+"-"+str(month)+"-"+spt[0]+" "+hour+":"+min+":"+sec
    return datetime

def main(input_dir,keyspace,table):
    text = sc.textFile(input_dir).repartition(8)
    log_lines = text.map(strip_regex).filter(lambda x: len(x)==6).map(transform).map(lambda x: Row(host=x[1],id=x[0],bytes=x[2],datetime=x[3],path=x[4]))

    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    log_corr = spark.createDataFrame(log_lines)
    log_corr.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()



if __name__ == '__main__':
    conf = SparkConf().setAppName('spark cassandra')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir,keyspace,table)

import sys
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, functions, types

def betacal(slr):
    num2 = (1/slr['sum_n']) * slr['sum_x'] * slr['sum_y']
    num = slr['sum_xy'] - num2
    den2 = (1/slr['sum_n']) * (slr['sum_x'] ** 2)
    den = slr['sum_x2'] - den2
    beta = num / den
    return beta

def alphacal(slr):
    term1 = (1/slr['sum_n']) * slr['sum_y']
    term2 = slr['beta'] * (1/slr['sum_n']) * slr['sum_x']
    alpha = term1 - term2
    return alpha

def main(topic):

    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    split_col = functions.split(values['value'],' ')
    x_y = values.withColumn('x',split_col.getItem(0)).withColumn('y',split_col.getItem(1)).drop(values['value'])

    x_y = x_y.withColumn('for_n', x_y['x']/x_y['x']).withColumn('x*y', x_y['x']*x_y['y']).withColumn('xp2', x_y['x']**2)
    x_y.createOrReplaceTempView("x_y")
    slr = spark.sql("SELECT sum(x) as sum_x,sum(y) as sum_y,sum(x*y) as sum_xy,sum(for_n) as sum_n,sum(xp2) as sum_x2 FROM x_y")

    beta = slr.withColumn('beta',betacal(slr))
    alpha = beta.withColumn('alpha',alphacal(beta))
    final = alpha.select('beta','alpha')
    
    stream = final.writeStream.format('console').outputMode('complete').start()
    stream.awaitTermination(300)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('structured streaming').getOrCreate()
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    topic = sys.argv[1]
    main(topic)

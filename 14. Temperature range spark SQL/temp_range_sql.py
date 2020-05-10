import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as fu, types
spark = SparkSession.builder.appName('Weather ETL').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),])


    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("weather")
    tmaxmin = spark.sql("SELECT date,station,value/10 as value FROM weather WHERE qflag IS NULL AND observation IN ('TMAX','TMIN')")

    tmaxmin.createOrReplaceTempView("tmaxmin")
    tempmaxmin = spark.sql("SELECT date,station,max(value) as tmax,min(value) as tmin FROM tmaxmin GROUP BY date,station")

    tempmaxmin.createOrReplaceTempView("tempmaxmin")
    with_range = spark.sql("SELECT date, station, tmax-tmin as range FROM tempmaxmin")

    with_range.createOrReplaceTempView("with_range")
    range_max = spark.sql("SELECT date,max(range) as max_range FROM with_range GROUP BY date")

    range_max.createOrReplaceTempView("range_max")
    joined = spark.sql("SELECT wr.date,wr.station,wr.range,rm.max_range FROM with_range wr INNER JOIN range_max rm USING(date)")

    joined.createOrReplaceTempView("joined")
    max_station = spark.sql("SELECT date, station,range FROM joined WHERE range == max_range ORDER BY date,station")

    max_station.coalesce(1).write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

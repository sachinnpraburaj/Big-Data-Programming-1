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

    qflag_notnull = weather.filter(weather.qflag.isNull())

    tmaxmin = qflag_notnull.filter(qflag_notnull.observation.isin(['TMAX','TMIN']))

    temp_in_C = tmaxmin.withColumn('value_in_c',tmaxmin.value / 10)

    subset = temp_in_C.select('date','station','value_in_c').cache()

    tempmaxmin = subset.groupBy(subset['date'],subset['station']).agg(fu.max(subset['value_in_c']).alias('tmax'),fu.min(subset['value_in_c']).alias('tmin'))

    with_range = tempmaxmin.withColumn('range', tempmaxmin['tmax'] - tempmaxmin['tmin']).cache()

    range_max = with_range.groupBy(with_range['date']).agg(fu.max(with_range['range']).alias('max_range'))

    joined = with_range.join(fu.broadcast(range_max),['date'],"inner")

    max_station = joined.filter(joined['range'] == joined['max_range']).drop(joined['max_range']).orderBy(joined['date'],joined['station']).select('date','station','range')

    max_station.coalesce(1).write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

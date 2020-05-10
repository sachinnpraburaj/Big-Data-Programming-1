import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def schema():
    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),])
    return tmax_schema

def yes_tmax():
    query = "SELECT today.station, today.date, today.latitude, today.longitude, today.elevation, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
    return query

def ret_query(table_name='__THIS__'):
    query = "SELECT station, dayofyear(date) AS dayofyear, latitude, longitude, elevation, tmax, yesterday_tmax FROM {table_name}".format(table_name=table_name)
    return query

def main(inputs,model_file):
    data = spark.read.csv(inputs, schema=schema())
    train, validation = data.randomSplit([0.75, 0.25],seed=42)

    sql_transformer1 = SQLTransformer(statement = yes_tmax())
    sql_transformer2 = SQLTransformer(statement = ret_query())
    assemble_features = VectorAssembler(inputCols=['latitude','longitude','elevation','dayofyear','yesterday_tmax'], outputCol='features')
    regressor = GBTRegressor(featuresCol='features', labelCol='tmax')
    pipeline = Pipeline(stages=[sql_transformer1,sql_transformer2,assemble_features, regressor])

    model = pipeline.fit(train)
    predictions = model.transform(validation)
    model.write().overwrite().save(model_file)

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print("R-squared value : "+str(r2))
    print("RMSE value : "+str(rmse))


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs,model_file)

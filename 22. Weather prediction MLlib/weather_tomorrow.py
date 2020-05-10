import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    inputrow = [('bigDataLab',datetime.strptime('2018-11-12', '%Y-%m-%d').date(),49.2771,-122.9146,float(330),float(12)),('bigDataLab',datetime.strptime('2018-11-13', '%Y-%m-%d').date(),49.2771,-122.9146,float(330),float(0))]
    test_tmax = spark.createDataFrame(inputrow, schema=tmax_schema)
    test_tmax.show()
    model = PipelineModel.load(model_file)
    predictions = model.transform(test_tmax)
    predictions.show()

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)
    print("prediction : "+ str(predictions.rdd.collect()[0][8]))


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)

# r2 = -inf
# rmse = 8.405464720821223
# (5,[0,1,2,3,4],[0.122864624178,0.0855345559493,0.0801739173575,0.238469793481,0.472957109034])

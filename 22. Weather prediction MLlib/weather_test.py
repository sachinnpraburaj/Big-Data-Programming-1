import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
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


def test_model(model_file, inputs):
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)
    model = PipelineModel.load(model_file)
    predictions = model.transform(test_tmax)
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('r2 =', r2)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    inputs = sys.argv[2]
    test_model(model_file, inputs)

# tmax-1
# without
# r2 = 0.4223307627519185
# rmse = 9.858522490578231
# (4,[0,1,2,3],[0.201499752659,0.145078973636,0.198710051307,0.454711222399])
# with
# r2 = 0.8393091808807551
# rmse = 5.18260238977464
# (5,[0,1,2,3,4],[0.122864624178,0.0855345559493,0.0801739173575,0.238469793481,0.472957109034])

# tmax-2
# r2 = 0.7767955637542446
# rmse = 6.128068656137969
# (4,[0,1,2,3],[0.303659620071,0.221240622776,0.149842548811,0.325257208342])
# r2 = 0.909761967485196
# rmse = 3.8837133465630247
# (5,[0,1,2,3,4],[0.218240881859,0.111887149257,0.0835111191793,0.291359545439,0.295001304266])

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def rgb_classify(type,train,validation,figName):
    rgb_assembler = VectorAssembler(inputCols=['R','G','B'],outputCol='features')
    word_indexer = StringIndexer(inputCol='word',outputCol='label',stringOrderType='alphabetAsc')
    if (type == "MLPC"):
        classifier = MultilayerPerceptronClassifier(layers=[3, 25, 25],seed=42)
    elif (type == "LogReg"):
        classifier = LogisticRegression()
    rgb_pipe = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipe.fit(train)
    predictions = rgb_model.transform(validation)
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    score = evaluator.evaluate(predictions)
    plot_predictions(rgb_model, 'RGB_'+figName, labelCol='word')
    return score

def lab_classify(type,train,validation,query,figName):
    sql_transformer = SQLTransformer(statement = query)
    lab_assembler = VectorAssembler(inputCols=['labL','labA','labB'],outputCol='features')
    word_indexer = StringIndexer(inputCol='word',outputCol='label',stringOrderType='alphabetAsc')
    if (type == "MLPC"):
        classifier = MultilayerPerceptronClassifier(layers=[3, 25, 25],seed=42)
    elif (type == "LogReg"):
        classifier = LogisticRegression()
    lab_pipe = Pipeline(stages=[sql_transformer,lab_assembler, word_indexer, classifier])
    lab_model = lab_pipe.fit(train)
    predictions = lab_model.transform(validation)
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')
    score = evaluator.evaluate(predictions)
    plot_predictions(lab_model, 'LAB_'+figName, labelCol='word')
    return score

def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25],seed=42)
    train = train.cache()
    validation = validation.cache()

    score_mpc = rgb_classify("MLPC",train,validation,"MLPC")
    score_Log = rgb_classify("LogReg",train,validation,"LogReg")

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    score_lab_mpc = lab_classify("MLPC",train,validation,rgb_to_lab_query,"MLPC")
    score_lab_Log = lab_classify("LogReg",train,validation,rgb_to_lab_query,"LogReg")

    print('Accuracy for RGB model using MultilayerPerceptronClassifier: %g' % (score_mpc, ))
    print('Accuracy for RGB model using LogisticRegression: %g' % (score_Log, ))
    print('Accuracy for LAB model using MultilayerPerceptronClassifier: %g' % (score_lab_mpc, ))
    print('Accuracy for LAB model using LogisticRegression: %g' % (score_lab_Log, ))


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)

# Accuracy for RGB model using MultilayerPerceptronClassifier: 0.592708
# Accuracy for RGB model using LogisticRegression: 0.726042
# Accuracy for LAB model using MultilayerPerceptronClassifier: 0.723958
# Accuracy for LAB model using LogisticRegression: 0.707292

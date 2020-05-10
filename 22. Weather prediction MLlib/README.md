## Executing the program

-- **Training the model**

```
${SPARK_HOME}/bin/spark-submit weather_train.py tmax-1 weather-model
```

-- **Testing the model**

```
${SPARK_HOME}/bin/spark-submit weather_test.py weather-model tmax-test
```

-- **Prediction**

```
${SPARK_HOME}/bin/spark-submit weather_tomorrow.py weather-model
```

## Executing the program

-- **Running the job**

```
${SPARK_HOME}/bin/spark-submit wordcount-imporved2.py wordcount-1 output-1
time ${SPARK_HOME}/bin/spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=8 wordcount-imporved2.py wordcount-1 output-1
```

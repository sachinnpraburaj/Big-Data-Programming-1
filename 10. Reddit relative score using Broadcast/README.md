## Executing the program

-- **Running the job**

```
${SPARK_HOME}/bin/spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=4 --executor-cores=4 --executor-memory=2g relative_score_bcast.py reddit-1 output-1
```

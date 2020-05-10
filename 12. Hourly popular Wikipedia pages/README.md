## Executing the program

-- **Running the job**

```
${SPARK_HOME}/bin/spark-submit --conf spark.sql.autoBroadcastJoinThreshold=-1 wikipedia_popular_df.py pagecounts-1 output-1
```

## Executing the program

-- **Running the job**

```
${SPARK_HOME}/bin/spark-submit wordcount.py wordcount-1 output-1
```

-- **Inspecting the output**

```
hdfs dfs -ls output-1
hdfs dfs -cat output-1/part-r-00000 | less
```

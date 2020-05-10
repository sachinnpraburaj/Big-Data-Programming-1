## Executing the program

-- **Running the job**

```
${SPARK_HOME}/bin/spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 correlate_logs_cassandra.py <keyspace> <table>
```

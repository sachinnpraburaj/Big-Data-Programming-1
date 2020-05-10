## Executing the program

-- **Compiling Java Code**

```
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WikipediaPopular.java
${JAVA_HOME}/bin/jar cf wiki.jar *.class
```

-- **Running the job**

```
${HADOOP_HOME}/bin/yarn jar wiki.jar WikipediaPopular pagecounts-with-time-0 output-1
${HADOOP_HOME}/bin/yarn jar wiki.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-with-time-0 output-1
```

-- **Inspecting the output**

```
hdfs dfs -ls output-1
hdfs dfs -cat output-1/part-r-00000 | less
```

## Executing the program

-- **Compiling Java Code**

```
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WordCountImproved.java
${JAVA_HOME}/bin/jar cf wordcount.jar WordCountImproved*.class
```

-- **Running the job**

```
${HADOOP_HOME}/bin/yarn jar wordcount.jar WordCountImproved wordcount-1 output-1
${HADOOP_HOME}/bin/yarn jar wordcount.jar WordCountImproved -D mapreduce.job.reduces=0 wordcount-1 output-1
```

-- **Inspecting the output**

```
hdfs dfs -ls output-1
hdfs dfs -cat output-1/part-r-00000 | less
hdfs dfs -cat output-1/part* | grep -i "^better"
```

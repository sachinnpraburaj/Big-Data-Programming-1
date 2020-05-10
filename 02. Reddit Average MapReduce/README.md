## Executing the program

-- **Compiling Java Code**

```
export HADOOP_CLASSPATH=./json-20180813.jar
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` LongPairWritable.java RedditAverage.java
${JAVA_HOME}/bin/jar cf a1.jar *.class
```

-- **Running the job**

```
${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage -libjars json-20180813.jar reddit-1 output-1
${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage -libjars json-20180813.jar -D mapreduce.job.reduces=0 reddit-1 output-1
```

-- **Inspecting the output**

```
hdfs dfs -ls output-1
hdfs dfs -cat output-1/part-r-00000 | less
```

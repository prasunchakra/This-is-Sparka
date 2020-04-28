# Spark Configure Linux

Get required version of kafka from http://spark.apache.org/downloads.html We are using spark-2.4.5-bin-hadoop2.7

edit and source .profile

```
export SPARK_HOME="/home/prasun/spark/spark-2.4.5-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"
```
If you want to open the REPL in jupyter notebook
```
export PYSPARK_SUBMIT_ARGS="pyspark-shell"
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark
```
Standalone Installation does not require to have Hadoop installed
 
Having all set up
* Open REPL with jupyter notebook using ```pyspark```
* Submit a python file to process using ```spark-submit xyz.py```



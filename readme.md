This project uses the spark GraphX to construct a graph of the votes of wiki pages and finds out the various metrics for each of the nodes of the graph. 

This project was coded using the IntelliJ IDEA and executed in AWS EMR cluster. 

Steps for implementing the project : 

1.	Open AWS Elastic Map Reduce (EMR) and create a cluster containing Hadoop and spark. emr5.31 is preferred. 
2.	Go to steps and click on add a step.
3.	Following are the arguments : 

spark-submit --deploy-mode cluster --class WikiGraph --jars s3://assignment2-kxn180028/graphframes-0.8.1-spark2.4-s_2.11.jar s3://assignment2-kxn180028/kafka_2.11-0.1.jar s3://assignment2-kxn180028/wiki-Vote.txt s3://assignment2-kxn180028/Assign3

Spark file : s3://assignment2-kxn180028/kafka_2.11-0.1.jar
Spark Arguments : s3://assignment2-kxn180028/wiki-Vote.txt
s3://assignment2-kxn180028/Assign3

4.	Result will be stored in a folder named Assign3 in s3. 

https://assignment2-kxn180028.s3.amazonaws.com/Assign3/part-00000



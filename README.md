
## Getting the setup ready:

1. Download Scala IDE and install m2e connectors from http://alchim31.free.fr/m2e-scala/update-site

2. Create github account and create a fork from https://github.com/niketanpansare/mljoin

3. File > Import > Git > Project froom Git > Clone URI > Put git path of your fork and your username/password

4. Import as general project

5. Right click on project > Configure > convert to maven project

6. Your setup should be seen as follow (i.e. both scala/java files are compilable units):

![Setup screenshot](/ScreenShot.jpg?raw=true "Setup screenshot")

7. mvn package (either through eclipse or commandline)

## Start Spark EC2 cluster with spot instances:

```python
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Request spot instances:
spark-1.5.2-bin-hadoop2.6/ec2/spark-ec2 -k JacobExperiments -i JacobExperiments.pem -s 2 launch jacobExperiments --instance-type=m3.large --spot-price=0.02

# Check EC2 console if the cluster is created and ssh into the head node
ssh -i JacobExperiments.pem root@ec2-54-161-....

# Copy mljoin3-0.0.1-SNAPSHOT.jar onto the head node and run the example :)
```

## Running the example:

```python
spark-shell --master yarn-client --jars mljoin3-0.0.1-SNAPSHOT.jar
scala> mljoin.GMMData.testLocal(sc, 7077)
```

## Coding the model for mljoin:

Three types of mljoin methods are supported:

1. naive

2. global

3. local

To use the mljoin methods, the user has to write three java classes that implements the interfaces:

1. Model: The method `Model process()` is called on the given model before joining the output of this method is used to join with the data.

2. Data: The method `Output process(Model m)` is called on the each processed model and data which share the same key.

3. Output

See src>main>java>GMMData> `public static RDD<Output> testLocal(SparkContext sc, int listenerPortNumber)` method for example invocation.
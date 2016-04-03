
## Getting the setup ready:

1. Download Scala IDE and install m2e connectors from http://alchim31.free.fr/m2e-scala/update-site

2. Create github account and create a fork from https://github.com/niketanpansare/mljoin

3. File > Import > Git > Project froom Git > Clone URI > Put git path of your fork and your username/password

4. Import as general project

5. Right click on project > Configure > convert to maven project

6. Your setup should be seen as follow (i.e. both scala/java files are compilable units):

![Setup screenshot](/ScreenShot.jpg?raw=true "Setup screenshot")

7. mvn package (either through eclipse or commandline)

## Running the example:

```python
spark-shell --master yarn-client --jars mljoin3-0.0.1-SNAPSHOT.jar
scala> mljoin.GMMData.testLocal(sc, 7077)
```

## Coding the model:

To use the mljoin methods, the user has to write three java classes that implements the interfaces:
1. Model: The method `Model process()` is called on the given model before joining the output of this method is used to join with the data.
2. Data: The method `Output process(Model m)` is called on the each processed model and data which share the same key.
3. Output

See src>main>java>GMMData> `public static RDD<Output> testLocal(SparkContext sc, int listenerPortNumber)` method for example invocation.
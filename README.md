
Getting the setup ready:
1. Download Scala IDE and install m2e connectors from http://alchim31.free.fr/m2e-scala/update-site
2. Create github account and create a fork from https://github.com/niketanpansare/jacobJoinAndHash
3. File > Import > Git > Project froom Git > Clone URI > Put git path of your fork and your username/password
4. Import as general project
5. Right click on project > Configure > convert to maven project
6. mvn package (either through eclipse or commandline)

Running the example:
spark-shell --master yarn-client --jars mljoin3-0.0.1-SNAPSHOT.jar
scala> mljoin.GMMData.testLocal(sc, 7077)

Coding the model:
See src>main>java>GMMData> "public static RDD<Output> testLocal(SparkContext sc, int listenerPortNumber)" method.
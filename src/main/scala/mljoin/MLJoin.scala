package mljoin

import breeze.linalg._
import breeze.numerics._
import org.apache.spark.broadcast._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import java.util.ArrayList
import java.util.HashSet
import java.util.Random
import org.apache.spark.{ Logging, SparkConf }
//import akka.actor.{ Actor, ActorRef, ActorSystem, ActorSelection }
//import akka.actor.Props
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
//import akka.actor.{ Props, Deploy, Address, AddressFromURIString }
//import akka.remote.RemoteScope
import java.util.concurrent.ConcurrentHashMap
import java.io._
import java.net._
import scala.collection.JavaConversions._

case class Message(val m:Model, val d:Data, val k:Int) extends Serializable
trait Model extends Serializable {
  def process(): Model
}
trait Data extends Serializable {
  def process(m:Model):Output
}
trait Output extends Serializable

class MLJoin extends Logging with Serializable {

	// More robust than naive2 as it doesnot require all the models to fit into driver memory
	def naive1(sc:SparkContext, models:RDD[(Integer, Model)], data:RDD[(Integer, Data)]): RDD[Output] = {
		models.join(data).map(x => {
			  val data = x._2._2
			  val processedModel = x._2._1.process
			  data.process(processedModel)
		} )
	}
	
	// It is likely faster than naive1 if Spark's groupBy mechanism is slower than its broadcast mechanism for given dataset
	// It is slower than global when f(V1:Model) is expensive
	// Requires that all the models to fit into driver memory
	def naive2(sc:SparkContext, models:RDD[(Integer, Model)], data:RDD[(Integer, Data)]): RDD[Output] = {
	  val allModels:Map[Integer, Model] = models.collect().toMap
	  val m = sc.broadcast(allModels)
	  data.map(x => {
	    val data = x._2
	    val processedModel = m.value.getOrElse(x._1, null).process 
	    data.process(processedModel)
	  })
	}
	
	// Fast than naive2 when f(V1:Model) is expensive
	// Requires that all the models to fit into driver memory
	def global(sc:SparkContext, models:RDD[(Integer, Model)], data:RDD[(Integer, Data)]): RDD[Output] = {
	  val evalModels:Map[Integer, Model] = models.map(x => (x._1, x._2.process)).collect().toMap
	  val m = sc.broadcast(evalModels)
	  data.map(x => {
		  val data = x._2
		  val processedModel = m.value.getOrElse(x._1, null)
		  if(processedModel == null) {
		    throw new RuntimeException("Processed model is not available in global approach")
		  }
		  data.process(processedModel)
	  })
	}
	
	def sendObject(msg:Message, host:String, port:Int) {
	  val socket = new Socket(host, port)
	  val out = new ObjectOutputStream(socket.getOutputStream())
	  out.writeObject(msg)
	  out.close
	  socket.close
	}
	
	def doSendModel(m:Model, k:Int, host:String, port:Int) {
	  sendObject(new Message(m , null, k), host, port)
	}
	
	def doSendData(d:Data, k:Int, host:String, port:Int) {
	  sendObject(new Message(null, d, k), host, port)
	}
	
	def sendModel(label:String, k:Int, m:Model) =  {
	  val elem = label.split(":")
	  doSendModel(m , k, elem(0), elem(1).toInt)
	}
	
	def sendData(label:String, k:Int, d:Data) = {
	  val elem = label.split(":")
	  doSendData(d, k, elem(0), elem(1).toInt)
	}
	
	
	
	def getNodeLabel(mapKNodes:Broadcast[scala.collection.mutable.Map[String, HashSet[Int]]], x:Int):String = {
	    for ((k,v) <- mapKNodes.value) {
	      if(v.contains(x)) return k 
	    }
	    throw new RuntimeException("The ID " + x + " is not labeled")
	}
	
	// : RDD[String]
	// Note: In this prototype, we donot shutdown the listeners gracefully (i.e. close port, clear Handler's variables, etc)
	// So, one can only invoke local once every job.
	def local(sc:SparkContext, models:RDD[(Integer, Model)], data:RDD[(Integer, Data)], listenerPortNumber:Int,
			processModelBeforeSending:Boolean): RDD[Output] = {   
	  val nodeLabels = sc.getExecutorMemoryStatus.map( x => 
	    x._1.split(":")(0) + ":" + listenerPortNumber
	  ).toList
	  
	  // -------------------------------------------------------
	  // Map nodes' ipAddress:portNumber to models's keys
	  
	  val listOfKs = models.keys.collect.toList.toSet
	  if(listOfKs.size < nodeLabels.size) logWarning("Nike: Number of models < number of executors")
	  
	  val mapKNodes1 = scala.collection.mutable.Map[String, HashSet[Int]]()
	  for(label <- nodeLabels) {
	    mapKNodes1 += label -> new HashSet
	  }
	  val rand = new Random
	  for(k <- listOfKs) {
	    val randIndex = rand.nextInt(nodeLabels.size)
	    mapKNodes1.get(nodeLabels(randIndex)).get.add(k)
	  }
	  logWarning("Mapping remote actors at " + mapKNodes1.foldLeft("")((x,y) => x + " { " + y._1 + " -> " + 
	      y._2 + " }"))
	  
	  val mapKNodes = sc.broadcast(mapKNodes1)
	  // -------------------------------------------------------
	  
	  // Hashing to node is easy, but precise node assignment difficult
	  // May need to create custom communication layer here
	  // which shuffles the below RDD to the node with IP given in key
	  val labeledModels:RDD[(String, (Integer, Model))] = models.map(x => (getNodeLabel(mapKNodes, x._1), x)).persist(StorageLevel.MEMORY_AND_DISK)
	  val labeledData:RDD[(String, (Integer, Data))] = data.map(x => (getNodeLabel(mapKNodes, x._1), x)).persist(StorageLevel.MEMORY_AND_DISK)
	  
	  // Bad way to start the listeners :(
	  Handler.startListener(listenerPortNumber, processModelBeforeSending)
	  sc.parallelize(1 to nodeLabels.length*100).map(x => Handler.startListener(listenerPortNumber, processModelBeforeSending)).count
	  
	  // Now, shuffle the model to the hashed node and store in hash table
	  labeledModels.map(x => { 
	    Handler.startListener(listenerPortNumber, processModelBeforeSending)
	  	if(processModelBeforeSending)
	  		sendModel(x._1, x._2._1, x._2._2.process)
	  	else  
	  		sendModel(x._1, x._2._1, x._2._2); 
	  	null
	  }).count
	  
	  // Now, shuffle the model to the hashed node and store in hash table
	  // We can do something like fetch data if necessary
	  labeledData.map(x => { Handler.startListener(listenerPortNumber, processModelBeforeSending); sendData(x._1, x._2._1, x._2._2); x}).count
	  
	  // Collect the output
	  val driverOutput = sc.parallelize(Handler.outputBuffer)
	  driverOutput.count
	  Handler.reset
	  sc.parallelize(1 to nodeLabels.length*100).flatMap( x => 
	    {
	    	var ret = new ArrayList[Output]
	    	Handler.synchronized {
	    	  if(Handler.outputBuffer.size() > 0) {
	    	    ret = Handler.outputBuffer
	    	    Handler.reset
	    	  }
	    	}
	    	ret
	    }
	  ).union(driverOutput)
	}
	
	// (new gmmimpute.JoinNHash()).testLocal(sc, 7077)
//	def testLocal(sc:SparkContext, listenerPortNumber:Int) = {
//	  val rand = new Random
//	  val m = sc.parallelize(1 to 1000000).map(x => (rand.nextInt(20), (new GMMModel(null, null)).asInstanceOf[Model] ))
//	  val d = sc.parallelize(1 to 1000000).map(x => (rand.nextInt(20), (new GMMData(null)).asInstanceOf[Data]))
//	  local(sc, m ,d, listenerPortNumber, true)
//	}
	
}

class ParallelHandler(msg: Message, startListener:Boolean, port:Int, processModelBeforeSending:Boolean) extends Thread {
  override def run() {
    if(startListener) {
      Handler.doStartReceiver(port, processModelBeforeSending)
    }
    else {
	  	if(msg.m == null && msg.d != null) {
	      // Got data
	      System.out.println("Nike: Got data [" + msg.k + "]")
	      Handler.outputBuffer.add(msg.d.process(Handler.cachedModels.get(msg.k)))
	    }
	    else if(msg.m != null && msg.d == null) {
	      // Got model
	      System.out.println("Nike: Got model [" + msg.k + "]")
	      if(!processModelBeforeSending)
	        Handler.cachedModels.put(msg.k, msg.m.process)
	      else
	        Handler.cachedModels.put(msg.k, msg.m)
	    }
	    else {
	      throw new RuntimeException("Incorrect message")
	    }
    }
  }
}

object Handler {
	@volatile var startedListener = false
	@volatile var stopListener = false 
	// First implementation assumes that executor can hold the output and models in memory
	// Next implementation: make this disk-based
	val cachedModels:ConcurrentHashMap[Integer, Model] = new ConcurrentHashMap[Integer, Model]
	val outputBuffer:ArrayList[Output] = new ArrayList[Output] 
	
	def reset() = {
	  startedListener = false
	  stopListener = false
	  cachedModels.clear
	  outputBuffer.clear
	}
	
	def startListener(port:Int, processModelBeforeSending:Boolean) {
	  if(!startedListener) {
	    this.synchronized {
	      if(!startedListener) {
	    	// doStartReceiver(port)
	        (new ParallelHandler(null, true, port, processModelBeforeSending)).start()
	        // Thread.sleep(100)
	        startedListener = true
	      }
	    }
	  }
	}
	
	def doStartReceiver(port:Int, processModelBeforeSending:Boolean) {
	  val ssocket = new ServerSocket(port)
	  // TODO: Make accept() more graceful
	  // ssocket.setSoTimeout(10000)
	  while (!stopListener) {  
	    val clientSocket = ssocket.accept()
	    val in = new ObjectInputStream(clientSocket.getInputStream())
	    val msg:Message = in.readObject().asInstanceOf[Message]
	    (new ParallelHandler(msg, false, -1, processModelBeforeSending)).start()
	  }
	}
} 


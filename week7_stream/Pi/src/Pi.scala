import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Duration

import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Pi {
  
  
  
  
  def main(args: Array[String])  {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Pi").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 4242, StorageLevel.MEMORY_AND_DISK_SER)
    
    
    
    val values = lines.map(_.split(" "))
    val valuesDouble = values.map(f=>(f(0).toDouble,f(1).toDouble))  
        
    val boolStep = valuesDouble.map(f=>{
      ((f._1*f._1 + f._2*f._2 < 1),1.toDouble)
    })
    
    val reducedStep = boolStep.reduceByKey(_+_)
    
    
    /* 1er temps
    val E1 = reducedStep.map(f=>(f._2,f._2))
    val E2 = E1.reduce((t,f)=>{
      (t._2,f._2)
    }).map(f=>("Valeur de pi : " + (4*f._2)/((f._1)+(f._2))))
    E2.print()
   */
    
    /* Méthode Window
    val E1 = reducedStep.map(f=>(f._2,f._2))
    val E2 = E1.reduceByWindow((t,f)=>{
      (t._2,f._2)
    }, new Duration(2000), new Duration(10000)).map(f=>("Valeur de pi : " + (4*f._2)/((f._1)+(f._2))))
    E2.print()
    */
    
    /* 2eme avec K,V */
    val E1 = reducedStep.updateStateByKey(          
      (vals, state: Option[Double]) => state match {
        case None => Some(vals.reduce(_+_))
        case Some(n) => Some(n+vals.reduce(_+_))
    })
    
    val E2 = E1.map(f=>(f._2,f._2))
    val end = E2.reduce((t,f)=>{
      (t._2,f._2)
    }).map(f=>("Valeur de pi : " + (4*f._2)/((f._1)+(f._2))))
    
    end.print()
        
   
    
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
  

 
  
  
}
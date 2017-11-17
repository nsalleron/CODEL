package SparkPi

import scala.math.random
import org.apache.spark._

/* Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]){
    
    
    //val conf  = new SparkConf().setAppName("Spark Pi")
    // On fait ce type de conf pour être compatible eclipse :
    
    // Dans le cas local :
    val conf = new SparkConf().setAppName("SparkPi").setMaster("local[4]")
    
    // Dans le cas master :
    //val conf = new SparkConf().setAppName("SparkPi").setMaster("spark://127.0.0.1:7077")
    
    // Dans le cas yarn :
    //val conf = new SparkConf().setAppName("SparkPi").setMaster("yarn")
    
    
    
    val spark = new SparkContext(conf)

    val slices = if(args.length > 0) args(0).toInt else 100
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_+_)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
    
    
  }
}
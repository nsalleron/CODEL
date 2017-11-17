package Chiffre


import org.apache.spark._
import java.util.Date

object Chiffre extends App {
  
 
  val conf = new SparkConf().setAppName("Chiffre").setMaster("local[4]")
    
  val spark = new SparkContext(conf)
  
  var annee =  2013// new Date().getYear + 1900
  
  
  /* https://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark */
  val textFile = spark.textFile("hdfs://127.0.0.1:9000/ventes", 1)
  /* Calcul du chiffre d'affaire sur 12 mois*/
  val rdd = textFile.map( line => line.split(" "))
              .map( array => (array(0).split("_")(2),array(3),array(4),array(5)))
              .filter( array => if(array._1.toInt < annee) false else true)
  
  /* Calcul du CA */
  var resultCA : Float = 0
  rdd.foreach(tupe => resultCA += tupe._2.toFloat)
  println("Chiffre d'affaire : " + resultCA)
  
  /* Calcul du produit le plus vendu par categorie*/
  val rdd2 = rdd.map(tupe => (tupe._4,tupe._3))  // On fait des tuples (categorie,produit)
               .groupByKey()
               .mapValues(iter => iter.map( s => (s,1))        // On les map => wordcount  (produit, 1)
                                     .groupBy(_._1)            // On le regroupe suivant la clé, ici : produit
                                     .toList                   // On convert vers List (produit,List(1,1,1...)    
                                     .map( c => (c._1,c._2.size))  // On fait un map (produit,list.size)
                                     .maxBy( c => c._2))          //On prend le produit le plus vendu de notre list
                           
                                     
  rdd2.foreach(t => println(t))
    
}
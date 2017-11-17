package Mat

import org.apache.spark._
import org.apache.spark.rdd.RDD
import Mat.VectorInt
import Mat.MatrixIntAsRDD


object Main extends App {
    val conf = new SparkConf().setAppName("Matrice").setMaster("local[2]") 
    val sc = new SparkContext(conf)
    val num_partition = 4
    
    val mat=new Array[MatrixIntAsRDD](6)
    
    mat (0) = makeMatriceIntRdd("matrice1", num_partition ,sc)
    mat (1) = makeMatriceIntRdd("matrice1", num_partition ,sc)
    mat (2) = makeMatriceIntRdd("matrice2", num_partition ,sc)
    mat (3) = makeMatriceIntRdd("matrice3", num_partition ,sc)
    mat (4) = makeMatriceIntRdd("matrice4", num_partition ,sc)
    mat (5) = makeMatriceIntRdd("matrice5", num_partition ,sc)
    
    mat.foreach( m => assert(m.lines.getNumPartitions == num_partition) )
    assert(mat(1).lines.first() == new VectorInt(Array(1,1,-1)))
    assert(mat(5).lines.first() == new VectorInt(Array(2,8,2,10)))
    
    var i=0
    mat.foreach( m => {println("matrice "+i+" :\n"+m) ; i+=1 } )
    assert(mat(1).nb_lines == mat(0).nb_lines && mat(1).nb_columns == mat(0).nb_columns)
    
   
    
    assert(mat(1).get(3, 1) == 7)
    assert(mat(5).get(2, 3) == -2)
    
    
    
    assert(mat(1) == mat(0)) 
    assert(mat(0) == mat(1))
    assert(mat(1) != mat(3))
    assert(mat(3) != mat(5))
    assert(mat(5) != mat(2))
    
    
    assert(mat(1) + mat(0) == mat(4))
    
    
    
    
    
    assert(mat(5) == mat(4).transpose()) 
    assert(mat(1) != mat(4).transpose()) 
    assert(mat(0).transpose() == mat(1).transpose())
    
    
    assert(mat(1) * mat(2) == mat(3))
    println("endAssert")
    
  /*
     * 
			- 1er rdd (RDD[String]) : création à partir du  chier (textfile)
			- 2ème rdd (RDD[Array[Int]]) : on transforme chaque ligne du 1er rdd en array de
				Int (map)
			- 3ème rdd (RDD[VectorInt]) : on transforme chaque élément du 2ème rdd en Vec-
			torInt (map)
     * 
     */
  
  def makeMatriceIntRdd(file : String, nb_part : Int, sc : SparkContext ):MatrixIntAsRDD = {
    val etape1 = sc.textFile("hdfs://127.0.0.1:9000/" + file, nb_part)
    val etape2 = etape1.map(line => line.split(" ").map(t=>t.toInt))
    val etape3 = etape2.map(arInt => new VectorInt(arInt))
    
    /*
     * A partir de maintenant nous souhaitons nous assurer que la répartition des éléments soit la même quelque soit le fichier d'entrée :
			- 4ème rdd (RDD[(VectorInt,Long)]) : lier chaque élément du 3eme rdd avec son index (zipWithIndex)
			- 5ème rdd (RDD[(VectorInt,Long)]) : trier le 4ème rdd en fonction de l'index crois- sant (sortBy)
			- 6ème rdd (RDD[VectorInt]) : enlever la partie droite de chaque élément pour ne garder que le vecteur (map)
			- enfin construire et retourner une MatrixIntAsRDD à partir du 6ème rdd.
     */
    
    val etape4 = etape3.zipWithIndex()
    val etape5 = etape4.sortBy(x => x._2, true, nb_part)
    val etape6 = etape5.map(f=>f._1)
    
    new MatrixIntAsRDD(etape6)
  }
    
}
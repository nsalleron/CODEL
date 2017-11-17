package Mat
import org.apache.spark.rdd.RDD

class MatrixIntAsRDD(val tmp : RDD[VectorInt]) {
    
    val lines = tmp
  
    override def toString = {
        val sb = new StringBuilder ()
        lines.collect().foreach( line => sb.append ( line +"\n"))
        sb.toString ()
    }  
    
    
    val nb_lines : Long = {
      lines.collect().count(_ => true)
    }
    
    val nb_columns : Long = {
      lines.collect()(0).length
    }
    
    def get(i:Int,j:Int):Int = {
      lines.collect()(i).tab(j)
    }
    
    override def equals( a: Any ): Boolean = a match {

      case that : MatrixIntAsRDD if(that.lines.collect().deep == this.lines.collect().deep ) => true
      case that : MatrixIntAsRDD if(that.lines.collect().deep != this.lines.collect().deep ) => false
    }
    
    def +(other: MatrixIntAsRDD):MatrixIntAsRDD = {    
     new MatrixIntAsRDD(lines.zip(other.lines)
                             .map(f => f._1 + f._2))
    }
    
    def transpose() : MatrixIntAsRDD = {
      var rddOfSeq =  lines.map(f => f.tab.toSeq)
      var seqOfseqTransposed =  rddOfSeq.collect().toSeq.transpose
      var seqOfVectorInt = seqOfseqTransposed.map(f => new VectorInt(f.toArray))
      var end = lines.context.parallelize(seqOfVectorInt, lines.partitions.length) //permet d'avoir le nombre originel de partition
      new MatrixIntAsRDD(end)
    }
    
     def *(other: MatrixIntAsRDD):MatrixIntAsRDD = {  
        var A = this.transpose().lines //lines est de type RDD[VectorInt]
        var B = other.lines //lines est de type RDD[VectorInt]

        /*
        var CoupleAB = A.zip(B)
        var Array = CoupleAB.map(f=>f._1.prodD(f._2))
        var tito = Array.reduce((a:Array[VectorInt],b:Array[VectorInt]) => a.col)
        new MatrixIntAsRDD(lines.context.parallelize(end.toSeq, lines.partitions.size))
        */
        
        //Etape 1  
        var coupleAB = A.collect().zip(B.collect()) //coupleAB est de type: Array[(VectorInt, VectorInt)]
        //Etape 2 
        var arrayOfArrayOfVectorInt =  coupleAB.map(f=>f._1.prodD(f._2))
        //Normalment il y a 3 arrays ici, il faut donc addition le n de array 1 avec le n de array 2 etc
        //Etape3
        var seqArOfArOfVec = arrayOfArrayOfVectorInt.toSeq
       
        var end =  seqArOfArOfVec.reduce( (a:Array[VectorInt],b:Array[VectorInt]) => {
                var tmp = new Array[VectorInt](a.size)
                for( x <- 0 to a.size -1){
                        tmp(x) = a(x) + b(x)
                }
                tmp
        })
        //end.foreach(f=>println(f))
        new MatrixIntAsRDD(lines.context.parallelize(end.toSeq, lines.partitions.size))
}
    
    
}
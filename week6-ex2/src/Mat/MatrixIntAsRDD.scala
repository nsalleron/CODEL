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
			lines.count()
			//lines.collect().count(_ => true)
					}

					val nb_columns : Long = {
							lines.first().length
			}

			def get(i:Int,j:Int):Int = {
					//lines.collect()(i).tab(j)

					lines.zipWithIndex().filter(_._2 == i).first()._1.get(j)
			}

			override def equals( a: Any ): Boolean = a match {

				//case that : MatrixIntAsRDD if(that.lines.collect().deep == this.lines.collect().deep ) => true
				//case that : MatrixIntAsRDD if(that.lines.collect().deep != this.lines.collect().deep ) => false

			case other : MatrixIntAsRDD => {
				if(other.nb_lines != this.nb_lines || other.nb_columns != this.nb_columns)
					  return false
				var T2 = other.lines.zip(this.lines)
				var T3 = T2.filter(f => f._1.!=(f._2))
				//println("COUNT : "+T3.count() + " empty : " + T3.isEmpty())
				T3.isEmpty()
			  }			  
			case _ => false  

			}

			def +(other: MatrixIntAsRDD):MatrixIntAsRDD = {    
					new MatrixIntAsRDD(lines.zip(other.lines)
							.map(f => f._1 + f._2))
			}

			def transpose() : MatrixIntAsRDD = {


			
					
					
					val E1 = lines.zipWithIndex()
					val E2 = E1.flatMap(c=>c._1.elements.zipWithIndex.map(x=>(x._2,(c._2,x._1))))
					val E3 = E2.groupByKey()
					val E4 = E3.mapValues(it=>new VectorInt(it.toArray[(Long,Int)].sortBy(_._1).map(_._2)))
					val E5 = E4.sortByKey(true,E3.getNumPartitions)
					val E6 = E5.map(f=>f._2)
					
					
					new MatrixIntAsRDD(E6)
				
			}

			def *(other: MatrixIntAsRDD):MatrixIntAsRDD = {  


					var A = this.transpose().lines //lines est de type RDD[VectorInt]
					var B = other.lines //lines est de type RDD[VectorInt]

        		//Etape 1
					var CoupleAB = A.zip(B)
					//Etape 2
					var A1 = CoupleAB.map(f=>f._1.prodD(f._2))
					var A2 = A1.map(f=>f.zipWithIndex)
					var A3 = A2.flatMap(f=>f)
					var A4 = A3.map(f=>f.swap)
					var A5 = A4.reduceByKey(_ + _) 
					var end = A5.map(f => f._2)

					new MatrixIntAsRDD(end)

			}


}
package TP2

object Moyenne extends App {
  var mylist : List[(Int,Int)] = List((10,1),(12,2),(20,10))
  println(moyennePondérée(mylist))
  
  // and use a simple increment function
 
  def moyennePondérée(l:List[(Int,Int)]) : Double = {
    var somme = l.map((a) => a._1)
    var t = somme.reduce(_+_)
    //println(t)
    var coef = l.map((a) => a._2)
    var tot = coef.reduce(_+_)
    //println(tot)
    //somme/tot
    t/tot
    
  }
}
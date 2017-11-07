package TP2

object ex5 extends App {
  
  val ascending = (a:Int,b:Int) => if(a<b) true else false
  val descending = (a:Int,b:Int) => if(a<b) false else true
  val ar1 = Array(3,4,5,6,10) 
  val ar2 = Array (13 ,4 ,6 ,7 ,49)
  assert ( isSorted (ar1 , ascending ))
  assert (! isSorted (ar1 , descending )) 
  assert(!isSorted(ar1.reverse , ascending)) 
  assert(isSorted(ar1.reverse , descending))
  assert (! isSorted (ar2 , ascending ))
  assert (! isSorted (ar2 , descending )) 
  assert(!isSorted(ar2.reverse , ascending)) 
  assert(!isSorted(ar2.reverse , descending))
  
  def isSorted[A](as: Array[A], ordered: (A,A) => Boolean): Boolean = {
    var c = true
    as.reduce((a,b) => {
      //println("Comparaison de "+a + " avec " + b)
      c = c && ordered(a,b)
      println("("+a+","+b+") = "+c)
      b
    })
    println("END")
    c
    //as.red
  }
}
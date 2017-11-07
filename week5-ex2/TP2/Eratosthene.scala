package TP2

object Eratosthene extends App  {
  
  //var n = premier(100)
  //println(n)
  var list : List[Int] = List.range[Int](2, 100)
   list = f(list)
   println(list)
  
  
  def premier(n:Int) : List[Int] ={
    var list : List[Int] = List.range[Int](2, n)
    
    for( i <- list){
        //                   !- entier        != Si différent 0 il reste dans la liste : si vrai alors reste dans la liste
        list = list.filter( a => (a == i || a % i != 0))
                                 // ¡cas ou il se divise par lui même
    }
    
    list
  }
  
  /*
   * 
FONCTION f( liste entiers l )
SI 1er element au carre > dernier element
ALORS resultat l SINON
resultat = concatenation du 1er element avec
f( l sans le 1er element et tous les elements non multiples du 1er element )
FIN SI
FIN FONCTION
   * 
   */
  
  def f(l:List[Int]) :List[Int] = {
    var result = List[Int]()
    var tmpList:List[Int] = l
    var tmp = l(0)
    
    if ((tmp*tmp)  > l.last){
      return l
    }else{
      result = result :+ tmp
      println("result :"+result)
      tmpList = tmpList.drop(1)
      tmpList = tmpList.filter( a => (a == tmp || a % tmp != 0))
      println("tmpList :" + tmpList)
      result = result ::: (f(tmpList))
    }
    result
  }
}
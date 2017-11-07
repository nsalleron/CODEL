package TP2

object LetterCount extends App {
  var tmp = List("Hadoop est une plateforme distribuee" , "Spark en est une autre" , "scala est un langage", "Java aussi")
  println(letterCount(tmp))

  def letterCount(l:List[String]) : Int = {
    var words = l.flatMap(_.split(" "))
    var valWords = words.map((f:String) => f.length())
    var sum:Int = 0
    sum += valWords.reduce(_ + _)
    sum = 0
    sum += valWords.reduce((x,y) => x + y)
    sum 
  }
  
  
  
  
  
  
  
  
  
  
  
  
}
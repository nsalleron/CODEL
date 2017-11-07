

package TP2

object exo1 extends App {
  
  val catalogue = new Catalogue 
  catalogue.addProduit("peluche" , 10.5)
  catalogue.addProduit("tondeuse" , 250.6)
  catalogue.addProduit("table" , 90) 
  catalogue.addProduit("saladier", 20) 
  catalogue.addProduit("casserole", 35) 
  println(catalogue)
  
  var catalogue2 = catalogue.soldefor(10)
  println(catalogue2)
  catalogue2.soldeFNomme(10)
  println(catalogue2)
  catalogue2.soldeFAno(10)
  println(catalogue2)
  
  
  class Catalogue {
    var mymap : Map[String,Double] = Map()
    def addProduit ( name: String, price: Double){
      mymap += ( name -> price)
    }
    
    def delProduit( name : String){
      for( (x,y) <- mymap){
          if(x == name){
            mymap - x
          }
      }
    }
    
    def soldefor(percent:Int): Catalogue = {
       for( (x,y) <- mymap){
           mymap += (x -> (y - (y * percent/100)))
       }
       this
    }
    
    def applyPercent(price: Double, percent: Double) = price - (price * percent/100)
    
    def soldeFNomme(percent:Int){
      mymap = mymap.mapValues(applyPercent(_:Double,percent))
    }
    
    def soldeFAno(percent:Double){
      mymap = mymap.mapValues(_ * (1-(percent/100)))
    }
    
  
   
    
    override def toString() : String = {
      var jStringBuffer = new StringBuffer();
      for( (x,y) <- mymap){
          jStringBuffer.append(x+" : "+y + " euros\n")
      }
      jStringBuffer.toString()
    }
  }
}
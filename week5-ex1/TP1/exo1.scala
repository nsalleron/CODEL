

package TP1

object exo1 extends App {
  
  val catalogue = new Catalogue 
  catalogue.addProduit("peluche" , 10.5)
  catalogue.addProduit("tondeuse" , 250.6)
  catalogue.addProduit("table" , 90) 
  catalogue.addProduit("saladier", 20) 
  catalogue.addProduit("casserole", 35) 
  println(catalogue)
  
  
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
    
   
    
    override def toString() : String = {
      var jStringBuffer = new StringBuffer();
      for( (x,y) <- mymap){
          jStringBuffer.append(x+" : "+y + " euros\n")
      }
      jStringBuffer.toString()
    }
  }
}
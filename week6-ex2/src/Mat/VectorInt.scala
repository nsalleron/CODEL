package Mat

import scala.collection.immutable.Vector._
import org.apache.spark._


class VectorInt(val elements:Array[Int]) extends Serializable{
  
  implicit def ArrayToVectorInt(t:Array[Int])=new VectorInt(t)
  def get(i:Int):Int = elements(i)
  def length:Int = elements.length
  def +(other:VectorInt):VectorInt = elements.zip(other.elements).map( x=> x._1+x._2 )
  //def -(other:Vector):Vector = elements.zip(other.elements).map( x=> x._1-x._2 )
  def *(v:Int):VectorInt = elements.map(_*v)
  
  override def equals(a:Any):Boolean = a match {
    case(o:VectorInt) =>  { 
      if (o.length != length) return false
      for(i <- 0 to elements.length-1){
        if(get(i) != o.get(i)) return false
      }
      return true
    }
    case _ => false
    
   
  }
  
  def prodD(other:VectorInt):Array[VectorInt]={
    val res = new Array[VectorInt](length)
    for(i <- 0 to res.length-1){
      res(i) = other.*(get(i))
    }
    return res
  }
  
  override def toString = {
    val sb = new StringBuilder("(")
    elements.foreach { x => sb.append(" "+x+" ") }
    sb.append(")")
    sb.toString()
  }
}

/*
class VectorInt(val tab : Array[Int]) extends java.io.Serializable{
    class IntVector(val original : Array[Int]) { 
      def myVector = original.toVector
    }
    implicit def tabIntToVector(value : Array[Int]) = new IntVector(value)
    
    def length : Int = tab.myVector.size
    
    def get( i : Int) : Int = tab(i)
    
    override def toString : String = {
      var jStringBuffer = new StringBuffer();
      jStringBuffer.append("( ")
      for ( i <- 0 to (tab.length - 1)){
        jStringBuffer.append(tab(i)+" ")
      }
      jStringBuffer.append(")")
      jStringBuffer.toString()
    }
    
    override def equals( a: Any ): Boolean = a match {
      case that : VectorInt if(that.tab.myVector.equals(this.tab.myVector) ) => true
      case that : VectorInt if(! that.tab.myVector.equals(this.tab.myVector) ) => false
    }
    
    def +(other: VectorInt ): VectorInt = {  
      var tmp : Array[Int] = Array.ofDim(other.length)
      for( i <- 0 to (tab.length -1)){
          tmp(i) = (other.tab(i)+this.tab(i))
      }
      new VectorInt(tmp) 
    }
    
     def *(other: Int ): VectorInt = {
      var tmp : Array[Int] = Array.ofDim(tab.length)
      for( i <- 0 to (tab.length -1)){
          tmp(i) = (other*this.tab(i))
      }
    
      new VectorInt(tmp) 
    }
    
    def prodD(other:VectorInt):Array[VectorInt] = {
      
      var tmpVector : Array[VectorInt] = Array.ofDim(tab.length)
      for( i <- 0 to (other.tab.length -1)){
        var tmp : Array[Int] = Array.ofDim(other.tab.length)
        for(j <- 0 to (tab.length - 1)){
          tmp(j) = (tab(i)*other.tab(j))
        }
        tmpVector(i) = new VectorInt(tmp);
      }
      return tmpVector
    }
    
 
  
  
  
 
  
  

}

*/


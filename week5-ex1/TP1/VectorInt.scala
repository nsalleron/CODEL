package TP1
import scala.collection.immutable.Vector._



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
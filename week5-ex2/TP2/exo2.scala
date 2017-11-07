

package TP2

object exo2 extends App {
  
    
      abstract class IntTree ;
      case object EmptyIntTree extends IntTree ;
      case class NodeInt(elem: Int, left: IntTree, right: IntTree) extends IntTree;
    
    var tree : IntTree = EmptyIntTree 
    for(v <- 1 to 10){
        tree=insert(tree ,v) 
    }
    println(size(tree))
    assert ( size ( tree ) == 10) 
    for(v <- 1 to 10){
      assert(contains(tree ,v)) 
    }
    for(v <- 11 to 20){
      assert (! contains(tree ,v))
    }
    println ("Ok")
    

    def contains(t: IntTree, v: Int): Boolean = t match {
      case EmptyIntTree                        => false
      case NodeInt(elem, a, b) if (elem == v)     => true
      case NodeInt(elem, left, b) if (elem < v)   => contains(b, v)
      case NodeInt(elem, a, right) if (elem > v)  => contains(a, v)
    }
     
    
    def size(t: IntTree):Int = t match {
      //case Node(elem, left, z)                 => 1 + size(left)
      //case Node(elem, z, right)                => 1 + size(right)
      case NodeInt(elem, left, right) if(left != EmptyIntTree && right != EmptyIntTree ) => 1 + size(left) + size(right)
      case NodeInt(elem, left, right) if(left != EmptyIntTree && right == EmptyIntTree ) => 1 + size(left)
      case NodeInt(elem, left, right) if(left == EmptyIntTree && right != EmptyIntTree ) => 1 + size(right)
      case NodeInt(elem, left, right) if(left == EmptyIntTree && right == EmptyIntTree ) => 1 
      
    }
    
    def insert(t: IntTree, v: Int): IntTree = t match { 
      case EmptyIntTree => new NodeInt(v, EmptyIntTree, EmptyIntTree)
      case NodeInt(elem, left, t) if (v < elem) => new NodeInt(elem, insert(left, v), t)
      case NodeInt(elem, left, right) => new NodeInt(elem, left, insert(right, v))
    }
      
    
    
    


}
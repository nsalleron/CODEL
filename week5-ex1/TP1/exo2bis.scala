

package TP1

object exo2bis extends App {
  
    abstract class Tree[+T] ;
    case object EmptyTree extends Tree ;
    case class Node[T](elem: Int, left: Tree[T], right: Tree[T]) extends Tree;
    
    var tree : Tree[Int] = EmptyTree 
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
    

    def contains[T](t: T, v: Int): Boolean = t match {
      case EmptyTree                        => false
      case Node(elem, a, b) if (elem == v)     => true
      case Node(elem, left, b) if (elem < v)   => contains(b, v)
      case Node(elem, a, right) if (elem > v)  => contains(a, v)
    }
     
    
    def size[T](t: T):Int = t match {
      //case Node(elem, left, z)                 => 1 + size(left)
      //case Node(elem, z, right)                => 1 + size(right)
      case Node(elem, left, right) if(left != EmptyTree && right != EmptyTree ) => 1 + size(left) + size(right)
      case Node(elem, left, right) if(left != EmptyTree && right == EmptyTree ) => 1 + size(left)
      case Node(elem, left, right) if(left == EmptyTree && right != EmptyTree ) => 1 + size(right)
      case Node(elem, left, right) if(left == EmptyTree && right == EmptyTree ) => 1 
      
    }
    
    def insert[T](t: Tree[T], v: Int): Tree[T] = t match { 
      case EmptyTree => new Node(v, EmptyTree, EmptyTree)
      case Node(elem, left, t) if (v < elem) => new Node(elem, insert(left, v), t)
      case Node(elem, left, right) => new Node(elem, left, insert(right, v))
    }
      
    
    
    


}
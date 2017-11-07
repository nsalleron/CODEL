package TP2

object ex6 extends App {
  
  def curryfie[A,B,C]( f: (A,B) => C ): A => B => C = {
    (a:A) => (b:B) => f(a,b) 
  }
  
  def decurryfie[A,B,C](f: A =>B =>C): (A, B) => C = {
    (a:A,b:B) => f(a)(b)
  }
  
  
  def plus(x:Int, y:Int) = x + y
  def fois(x:Int, y:Int) = x.*(y) 
  val p_curr = curryfie(plus)
  val p_decurr = decurryfie (p_curr) 
  val f_curr = curryfie(fois)
  val f_decurr = decurryfie(f_curr)
  assert(p_curr(3)(4) == p_decurr(3,4)) 
  assert(f_curr(6)(7) == f_decurr(6,7))
  println("done")
  
}
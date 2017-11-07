

package TP1

object exo3 extends App {
    def testVectorInt = {
      val v1 = new VectorInt(Array(1,4,1))
      val v2 = new VectorInt(Array(2,-1,4))
      val v3 = new VectorInt(Array(2,-1,4,6,10,-3))
      val v4 = new VectorInt(Array(1,4,1))
      
      //println("get sur v4 : "+ v4.get(1))
      
      assert(v1.length == 3)
      assert(v2.length == 3)
      assert(v3.length == 6)
      assert(v4.length == 3)
      //println("done length")
      assert (v4==v1)
      assert (v1==v1)
      assert (v4==v4)
      assert (v3!=v2)
      
      assert (v2!=v3)
      assert (v1!=v3)
      assert (v1!=v2)
     
      
    
      assert (v1 + v2 == new VectorInt(Array(3,3,5)))
      assert (v2 + v1 == new VectorInt(Array(3,3,5)))
      assert (v1 + v1 == v4 +v1)
      assert (v1 + v1 == v4 +v1)
      //println("done 1")
      
      
      assert (v3 * 10 == v3 + (v3*9) )
      assert (v2 * 2 == v2 + v2)
      
      v2.prodD(v1). foreach(println(_) )
      
    }
    testVectorInt
}
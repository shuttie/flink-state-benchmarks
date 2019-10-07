package me.dfdx.flinkstate.demo

import me.dfdx.flinkstate.serde.PrimitiveVarLenArraySerializer
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer
import org.apache.flink.core.memory.DataOutputViewStreamWrapper

object VarLenDemo {
  def main(args: Array[String]): Unit = {
    val count = 10000
    val default = new IntPrimitiveArraySerializer()
    val simple = new PrimitiveVarLenArraySerializer()

    val ints = for {
      i <- 1 to 31
      next = math.round(math.pow(2.0, i) - 1.0).toInt
      array = (0 to count).map(_ => next).toArray
    } yield {
      val a = len(array, default) / count
      val b = len(array, simple) / count
      println(s"$i $next $a $b")
    }
  }

  def len(values: Array[Int], ser: TypeSerializer[Array[Int]]) = {
    val buf = new CountingOutputStream(new NullOutputStream())
    val stream = new DataOutputViewStreamWrapper(buf)
    ser.serialize(values, stream)
    buf.getByteCount
  }
}

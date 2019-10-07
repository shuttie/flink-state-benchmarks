package me.dfdx.flinkstate.demo

import java.io.ByteArrayOutputStream

import me.dfdx.flinkstate.serde.PlainStringSerializer
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.core.memory.DataOutputViewStreamWrapper

object CustomStringDemo {
  def main(args: Array[String]): Unit = {
    val default = new StringSerializer()
    val simple = new PlainStringSerializer()
    println("default size = " + serialize("hello world")(default).length)
    println("simple size = " + serialize("hello world")(simple).length)
  }
  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    val buf = new ByteArrayOutputStream()
    val stream = new DataOutputViewStreamWrapper(buf)
    serializer.serialize(item, stream)
    buf.toByteArray
  }

}

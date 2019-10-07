package me.dfdx.flinkstate.demo

import java.io.ByteArrayOutputStream

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import io.findify.flinkadt.api.typeinfo._
import io.findify.flinkadt.api.serializer._
import io.findify.flinkadt.instances.all._
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.DataOutputViewStreamWrapper

object ADTDemo {
  case class Simple(a: String)
  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(a: Int) extends ADT

  def main(args: Array[String]): Unit = {
    val conf = new ExecutionConfig()
    val ti2 = implicitly[TypeInformation[ADT]].createSerializer(conf)
    val item: ADT = Foo("a")
    println(serialize(item).length)
  }
  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    val buf = new ByteArrayOutputStream()
    val stream = new DataOutputViewStreamWrapper(buf)
    serializer.serialize(item, stream)
    buf.toByteArray
  }

}

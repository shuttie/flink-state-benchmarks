package me.dfdx.flinkstate

import java.io.ByteArrayOutputStream

import me.dfdx.flinkstate.benchmark.SerdeBenchmark.{RichClass, SimpleClass}
import org.apache.commons.io.output.NullOutputStream
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.DataOutputViewStreamWrapper

import scala.util.Random


object KryoFlame {
  def main(args: Array[String]): Unit = {
    val conf = new ExecutionConfig()
    val kryoSerializer = implicitly[TypeInformation[RichClass]].createSerializer(conf)
    val buffer = new NullOutputStream()
    val stream = new DataOutputViewStreamWrapper(buffer)
    val item = new SimpleClass("a")
    for {
      _ <- 0 to 100000000
    } {
      val item = new RichClass("a", "b", Random.nextLong(), Array(1,2,3), true, new SimpleClass("c"))
      kryoSerializer.serialize(item, stream)
    }
    stream.flush()
    buffer.close()

  }
}

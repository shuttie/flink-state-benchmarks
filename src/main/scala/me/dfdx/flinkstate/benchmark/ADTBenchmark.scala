package me.dfdx.flinkstate.benchmark

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import io.findify.flinkadt.api.typeinfo._
import me.dfdx.flinkstate.benchmark.ADTBenchmark.{ADT, Foo}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataOutputView, DataOutputViewStreamWrapper}
import org.openjdk.jmh.annotations._
import io.findify.flinkadt.api.typeinfo._
import io.findify.flinkadt.api.serializer._
import io.findify.flinkadt.instances.all._
import org.apache.commons.compress.utils.CountingOutputStream
import org.openjdk.jmh.util.NullOutputStream

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class ADTBenchmark {
  val conf = new ExecutionConfig()
  val kryoSerializer = org.apache.flink.api.scala.createTypeInformation[ADT].createSerializer(conf)
  val fooSerializer = org.apache.flink.api.scala.createTypeInformation[Foo].createSerializer(conf)
  val coproductSerializer = implicitly[TypeInformation[ADT]].createSerializer(conf)

  var buf: CountingOutputStream = _
  var stream: DataOutputView = _

  val item: ADT = Foo("a")

  @Setup(Level.Iteration)
  def setup = {
    buf = new CountingOutputStream(new NullOutputStream())
    stream = new DataOutputViewStreamWrapper(buf)
  }
  @Benchmark
  def measureFoo = {
    serialize(item)(coproductSerializer)
  }

  @Benchmark
  def measureKryoADT = {
    serialize(item)(kryoSerializer)
  }

  @Benchmark
  def measureADT = {
    serialize(item)(coproductSerializer)
  }



  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    serializer.serialize(item, stream)
    buf.getBytesWritten()
  }

}

object ADTBenchmark {
  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(a: Int) extends ADT
}

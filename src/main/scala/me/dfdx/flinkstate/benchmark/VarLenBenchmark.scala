package me.dfdx.flinkstate.benchmark

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import me.dfdx.flinkstate.serde.{PlainStringSerializer, PrimitiveVarLenArraySerializer}
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer
import org.apache.flink.core.memory.{DataOutputView, DataOutputViewStreamWrapper}
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class VarLenBenchmark {
  val default = new IntPrimitiveArraySerializer()
  val simple = new PrimitiveVarLenArraySerializer()

  var buf: ByteArrayOutputStream = _
  var stream: DataOutputView = _
  var source: Array[Int] = _

  @Param(Array("7", "14", "21", "28", "31"))
  var BITS: String = _

  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream(1024)
    stream = new DataOutputViewStreamWrapper(buf)
    source = (0 to 100).map(_ => math.round(Math.pow(2.0, BITS.toDouble)-1.0).toInt).toArray
  }

  @Benchmark
  def measureDefault = {
    serialize(source)(default)
  }

  @Benchmark
  def measureVarInt = {
    serialize(source)(simple)
  }

  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    buf.reset()
    serializer.serialize(item, stream)
    buf.toByteArray
  }

}

package me.dfdx.flinkstate.benchmark

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import me.dfdx.flinkstate.serde.PlainStringSerializer
import org.apache.commons.compress.utils.CountingOutputStream
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.core.memory.{DataOutputView, DataOutputViewStreamWrapper}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.util.NullOutputStream

import scala.util.Random

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class StringSerializerBenchmark {
  val default = new StringSerializer()
  val simple = new PlainStringSerializer()

  var buf: ByteArrayOutputStream = _
  var stream: DataOutputView = _
  var item: String = _
  val chars = "qwertyuiopasdfghklzxcvbnm".toCharArray
  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream(32)
    stream = new DataOutputViewStreamWrapper(buf)
    item = (0 to 16).map(_ => chars(Random.nextInt(chars.length))).mkString("")
  }

  @Benchmark
  def measureDefault = {
    serialize(item)(default)
  }

  @Benchmark
  def measureSimple = {
    serialize(item)(simple)
  }

  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    buf.reset()
    serializer.serialize(item, stream)
    buf.toByteArray
  }
}

package me.dfdx.flinkstate.benchmark

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import me.dfdx.flinkstate.benchmark.SerdeBenchmark.{ADT, CustomClass, Foo, KryoSerializer, RichCase, RichClass, SimpleCase, SimpleClass}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.{DataOutputView, DataOutputViewStreamWrapper}
import org.openjdk.jmh.annotations._

import scala.util.Random

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class SerdeBenchmark {
  val conf = new ExecutionConfig()
  conf.registerTypeWithKryoSerializer(classOf[CustomClass], classOf[KryoSerializer])
  implicit val caseSerializer = implicitly[TypeInformation[SimpleCase]].createSerializer(conf)
  implicit val kryoSerializer = implicitly[TypeInformation[SimpleClass]].createSerializer(conf)
  implicit val customKryoSerializer = implicitly[TypeInformation[CustomClass]].createSerializer(conf)


  implicit val richCaseSerializer = implicitly[TypeInformation[RichCase]].createSerializer(conf)
  implicit val richClassSerializer = implicitly[TypeInformation[RichClass]].createSerializer(conf)

  implicit val adtSerializer = implicitly[TypeInformation[ADT]].createSerializer(conf)

  var buf: ByteArrayOutputStream = _
  var stream: DataOutputView = _

  var rc = RichCase("a", "b", Random.nextLong(), Array(1,2,3), true, SimpleCase("c"))
  var rl = new RichClass("a", "b", Random.nextLong(), Array(1,2,3), true, new SimpleClass("c"))

  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream()
    stream = new DataOutputViewStreamWrapper(buf)
  }

  @Benchmark
  def serializeCaseClass = serialize(SimpleCase("hello"))

  @Benchmark
  def serializeClass = serialize(new SimpleClass("hello"))

  @Benchmark
  def serializeCustom = serialize(new CustomClass("hello"))

  @Benchmark
  def serializeADT = {
    val item: ADT = Foo("hello")
    serialize(item)
  }

  @Benchmark
  def serializeRichCase = serialize(rc)

  @Benchmark
  def serializeRichClass = serialize(rl)

  def serialize[T](item: T)(implicit serializer: TypeSerializer[T]) = {
    serializer.serialize(item, stream)
    buf.size()
  }
}

object SerdeBenchmark {
  case class SimpleCase(a: String)
  class SimpleClass(val a: String)
  class CustomClass(val a: String)

  case class RichCase(user: String, item: String, ts: Long, events: Array[Int], flag: Boolean, nested: SimpleCase)
  class RichClass(val user: String, val item: String, val ts: Long, val events: Array[Int], val flag: Boolean, val nested: SimpleClass)

  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(a: Int) extends ADT

  class KryoSerializer extends Serializer[CustomClass] {
    override def read(kryo: Kryo, input: Input, `type`: Class[CustomClass]): CustomClass = {
      val field = input.readString()
      new CustomClass(field)
    }

    override def write(kryo: Kryo, output: Output, `object`: CustomClass): Unit = {
      output.writeString(`object`.a)
    }
  }
}
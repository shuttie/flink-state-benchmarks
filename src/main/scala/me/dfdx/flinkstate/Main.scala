package me.dfdx.flinkstate

import java.io.{ByteArrayOutputStream, File, FileOutputStream}

import me.dfdx.flinkstate.benchmark.SerdeBenchmark.{ADT, CustomClass, Foo, KryoSerializer, SimpleCase, SimpleClass}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.DataOutputViewStreamWrapper


object Main {
  def main(args: Array[String]): Unit = {
    val conf = new ExecutionConfig()
    conf.registerTypeWithKryoSerializer(classOf[CustomClass], classOf[KryoSerializer])
    val caseSerializer = implicitly[TypeInformation[SimpleCase]].createSerializer(conf)
    val kryoSerializer = implicitly[TypeInformation[SimpleClass]].createSerializer(conf)
    val customKryoSerializer = implicitly[TypeInformation[CustomClass]].createSerializer(conf)
    val adtSer = implicitly[TypeInformation[ADT]].createSerializer(conf)
    val item: ADT = Foo("a")
    val a = serialize(item, adtSer)
    val as = new FileOutputStream(new File("/tmp/adt.bin"))
    as.write(a)
    as.close()
    val kryo = serialize(new SimpleClass("a"), kryoSerializer)
    val ak = new FileOutputStream(new File("/tmp/ak.bin"))
    ak.write(kryo)
    ak.close()
    //val cse = serialize(new SimpleCase("a"), caseSerializer)
    val br=1
  }
  def serialize[T](item: T, serializer: TypeSerializer[T]): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val stream =  new DataOutputViewStreamWrapper(buffer)
    serializer.serialize(item, stream)
    stream.flush()
    buffer.toByteArray
  }

}

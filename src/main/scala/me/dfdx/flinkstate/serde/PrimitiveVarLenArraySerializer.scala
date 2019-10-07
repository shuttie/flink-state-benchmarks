package me.dfdx.flinkstate.serde

import io.findify.flinkadt.api.serializer.SimpleSerializer
import me.dfdx.flinkstate.VarInt
import me.dfdx.flinkstate.serde.PrimitiveVarLenArraySerializer.PrimitiveVarLenArraySerializerSnapshot
import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class PrimitiveVarLenArraySerializer  extends SimpleSerializer[Array[Int]] {
  override def createInstance(): Array[Int] = Array.emptyIntArray
  override def getLength: Int = -1
  override def deserialize(source: DataInputView): Array[Int] = {
    val len = VarInt.getVarInt(source)
    val buf = new Array[Int](len)
    var i = 0
    while (i < len) {
      buf(i) = VarInt.getVarInt(source)
      i += 1
    }
    buf
  }
  override def serialize(record: Array[Int], target: DataOutputView): Unit = {
    VarInt.putVarInt(record.length, target)
    var i = 0
    while (i < record.length) {
      VarInt.putVarInt(record(i), target)
      i += 1
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[Array[Int]] = PrimitiveVarLenArraySerializerSnapshot
}

object PrimitiveVarLenArraySerializer {
  case object PrimitiveVarLenArraySerializerSnapshot extends SimpleTypeSerializerSnapshot[Array[Int]](() => new PrimitiveVarLenArraySerializer())
}


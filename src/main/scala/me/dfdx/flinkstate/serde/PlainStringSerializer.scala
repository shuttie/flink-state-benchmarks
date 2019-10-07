package me.dfdx.flinkstate.serde

import io.findify.flinkadt.api.serializer.SimpleSerializer
import me.dfdx.flinkstate.serde.PlainStringSerializer.StringSerializerSnapshot
import org.apache.flink.api.common.typeutils.{SimpleTypeSerializerSnapshot, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class PlainStringSerializer extends SimpleSerializer[String] {
  override def createInstance(): String = ""
  override def getLength: Int = -1
  override def deserialize(source: DataInputView): String = {
    source.readUTF()
  }
  override def serialize(record: String, target: DataOutputView): Unit = {
    target.writeUTF(record)
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[String] = StringSerializerSnapshot
}

object PlainStringSerializer {
  case object StringSerializerSnapshot extends SimpleTypeSerializerSnapshot[String](() => new PlainStringSerializer())
}

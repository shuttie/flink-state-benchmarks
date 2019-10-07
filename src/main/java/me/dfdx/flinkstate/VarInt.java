package me.dfdx.flinkstate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * VarInt implementation taken from Bazel
 */

public class VarInt {



    /**
     * Reads a varint from the current position of the given ByteBuffer and
     * returns the decoded value as 32 bit integer.
     *
     * <p>The position of the buffer is advanced to the first byte after the
     * decoded varint.
     *
     * @param src the ByteBuffer to get the var int from
     * @return The integer value of the decoded varint
     */
    public static int getVarInt(DataInput src) throws IOException {
        int tmp;
        if ((tmp = src.readByte()) >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = src.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = src.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = src.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = src.readByte()) << 28;
                    while (tmp < 0) {
                        // We get into this loop only in the case of overflow.
                        // By doing this, we can call getVarInt() instead of
                        // getVarLong() when we only need an int.
                        tmp = src.readByte();
                    }
                }
            }
        }
        return result;
    }

    /**
     * Encodes an integer in a variable-length encoding, 7 bits per byte, to a
     * ByteBuffer sink.
     * @param v the value to encode
     * @param sink the ByteBuffer to add the encoded value
     */
    public static void putVarInt(int v, DataOutput sink) throws IOException {
        while (true) {
            int bits = v & 0x7f;
            v >>>= 7;
            if (v == 0) {
                sink.writeByte((byte) bits);
                return;
            }
            sink.writeByte((byte) (bits | 0x80));
        }
    }
}

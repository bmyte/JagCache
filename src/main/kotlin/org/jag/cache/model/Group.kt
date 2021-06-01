package org.jag.cache.model

import org.jag.cache.util.ByteBufferOutputStream
import org.jag.cache.util.IO
import org.jag.cache.util.XteaCipher
import java.nio.ByteBuffer

class Group private constructor(val compressor: Compressor, val data: ByteBuffer, val crc32: Int, val version: Int) {

    override fun toString(): String {
        return "Group(compressor=$compressor, data=$data, crc32=$crc32, version=$version)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Group) return false
        return crc32 == other.crc32 && compressor === other.compressor && version == other.version && data == other.data
    }

    override fun hashCode(): Int {
        return crc32
    }

    companion object {
        fun decompress(groupCompressed: ByteBuffer, key: IntArray?): Group {
            val start = groupCompressed.position()
            val compressor = Compressor.of(groupCompressed.get())
            val len: Int = groupCompressed.int + compressor.headerSize()
            var b = IO.getSlice(groupCompressed, len)
            val crc32: Int = IO.crc32(groupCompressed.duplicate().flip().position(start))
            if (key != null) XteaCipher.decrypt(IO.getBuffer(b).also { b = it }, key)
            val data = compressor.decompress(b)
            var version = 0
            if (groupCompressed.hasRemaining()) {
                version = java.lang.Short.toUnsignedInt(groupCompressed.short)
                require(!groupCompressed.hasRemaining())
            }
            return Group(compressor, data, crc32, version)
        }

        fun compress(compressor: Compressor, data: ByteBuffer, version: Int, key: IntArray): ByteBuffer {
            val out = ByteBufferOutputStream()
            out.write(compressor.id())
            val pos: Int = out.buf.position()
            out.writeInt(0)
            compressor.compress(data, out)
            out.buf.putInt(pos, out.buf.position() - pos - compressor.headerSize() - 4)
            XteaCipher.encrypt(out.buf.duplicate().flip().position(5), key)
            if (version != 0) out.writeShort(version.toShort())
            return out.buf.flip()
        }

        fun compress(compressor: Compressor, data: ByteBuffer, key: IntArray): ByteBuffer {
            return compress(compressor, data, 0, key)
        }
    }
}

package org.jag.cache.util

import java.io.OutputStream
import java.nio.ByteBuffer

class ByteBufferOutputStream @JvmOverloads constructor(initialCapacity: Int = 32) : OutputStream() {
    var buf = ByteBuffer.allocate(initialCapacity)

    private fun reserve(n: Int): ByteBuffer {
        if (buf.remaining() < n) {
            val cap = (buf.capacity() * 2).coerceAtLeast(buf.position() + n)
            buf = ByteBuffer.allocate(cap).put(buf.flip())
        }
        return buf
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        reserve(len).put(b, off, len)
    }

    override fun write(b: ByteArray) {
        write(b, 0, b.size)
    }

    override fun write(b: Int) {
        write(b.toByte())
    }

    fun write(b: Byte) {
        reserve(java.lang.Byte.BYTES).put(b)
    }

    fun writeShort(s: Short) {
        reserve(java.lang.Short.BYTES).putShort(s)
    }

    fun writeInt(n: Int) {
        reserve(Integer.BYTES).putInt(n)
    }

    fun write(buf: ByteBuffer) {
        reserve(buf.remaining()).put(buf)
    }

    override fun toString(): String {
        return "ByteBufferOutputStream(buf=$buf)"
    }
}

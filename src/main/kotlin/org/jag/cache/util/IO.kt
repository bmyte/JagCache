package org.jag.cache.util

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.lang.Exception
import java.nio.ByteBuffer
import java.nio.IntBuffer
import java.nio.ShortBuffer
import java.util.concurrent.CompletableFuture
import java.util.zip.CRC32

object IO {
    @Throws(IOException::class)
    fun readBytes(`in`: InputStream, dst: ByteArray) {
        readNBytes(`in`, dst, 0, dst.size)
    }

    @Throws(IOException::class)
    fun readNBytes(`in`: InputStream, dst: ByteArray?, off: Int, len: Int) {
        if (`in`.readNBytes(dst, off, len) != len) throw EOFException()
    }

    @Throws(IOException::class)
    fun transferTo(buf: ByteBuffer, out: OutputStream): Long {
        val len = buf.remaining()
        if (buf.hasArray()) {
            out.write(buf.array(), buf.arrayOffset() + buf.position(), len)
            buf.position(buf.limit())
        } else {
            out.write(getArray(buf, len))
        }
        return len.toLong()
    }

    fun closeQuietly(original: Throwable, closeable: AutoCloseable) {
        try {
            closeable.close()
        } catch (e: Exception) {
            original.addSuppressed(e)
        }
    }

    fun getBuffer(buf: ByteBuffer): ByteBuffer {
        return ByteBuffer.wrap(getArray(buf))
    }

    fun getArray(buf: ByteBuffer): ByteArray {
        return getArray(buf, buf.remaining())
    }

    fun getArray(buf: ByteBuffer, len: Int): ByteArray {
        val b = ByteArray(len)
        buf[b]
        return b
    }

    fun getSlice(buf: ByteBuffer, len: Int): ByteBuffer {
        val i = buf.position() + len
        val slice = buf.duplicate().limit(i)
        buf.position(i)
        return slice
    }

    fun getShortSlice(buf: ByteBuffer, len: Int): ShortBuffer {
        return getSlice(buf, len * java.lang.Short.BYTES).asShortBuffer()
    }

    fun getIntSlice(buf: ByteBuffer, len: Int): IntBuffer {
        return getSlice(buf, len * Integer.BYTES).asIntBuffer()
    }

    fun getMedium(buf: ByteBuffer): Int {
        return buf.short.toInt() shl 8 or (buf.get().toInt() and 0xFF)
    }

    fun putMedium(buf: ByteBuffer, value: Int) {
        buf.putShort((value shr 8).toShort()).put(value.toByte())
    }

    fun crc32(buf: ByteBuffer?): Int {
        val crc = CRC32()
        crc.update(buf)
        return crc.value.toInt()
    }

    fun join(vararg bufs: ByteBuffer?): ByteBuffer {
        if (bufs.size == 1) return bufs[0]!!
        var len = 0
        for (b in bufs) len += b!!.remaining()
        val buf = ByteBuffer.allocate(len)
        for (b in bufs) buf.put(b)
        return buf.flip()
    }

    fun allOf(cfs: Collection<CompletableFuture<*>>): CompletableFuture<Void> {
        return CompletableFuture.allOf(*cfs.toTypedArray())
    }
}
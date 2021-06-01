package org.jag.cache.model

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.jag.cache.util.ByteBufferInputStream
import org.jag.cache.util.ByteBufferOutputStream
import org.jag.cache.util.IO
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.SequenceInputStream
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

enum class Compressor {
    NONE {
        override fun decompress(compressed: ByteBuffer): ByteBuffer {
            return IO.getBuffer(compressed)
        }

        override fun compress(buf: ByteBuffer, dst: ByteBufferOutputStream) {
            dst.write(buf)
        }
    },
    BZIP2 {
        val BLOCK_SIZE = 1
        private val HEADER = byteArrayOf('B'.code.toByte(), 'Z'.code.toByte(), 'h'.code.toByte(), ('0'.code + BLOCK_SIZE).toByte())
        override fun decompress(compressed: ByteBuffer): ByteBuffer {
            val out = ByteArray(compressed.int)
            try {
                BZip2CompressorInputStream(
                    SequenceInputStream(ByteArrayInputStream(HEADER),ByteBufferInputStream(compressed))
                ).use { stream ->
                    IO.readBytes(stream, out)
                }
            } catch (e: IOException) {
                throw UncheckedIOException(e)
            }
            return ByteBuffer.wrap(out)
        }

        override fun compress(buf: ByteBuffer, dst: ByteBufferOutputStream) {
            val start: Int = dst.buf.position()
            val len = buf.remaining()
            try {
                BZip2CompressorOutputStream(dst, BLOCK_SIZE).use { out -> IO.transferTo(buf, out) }
            } catch (e: IOException) {
                throw UncheckedIOException(e)
            }
            dst.buf.putInt(start, len)
        }
    },
    GZIP {
        override fun decompress(compressed: ByteBuffer): ByteBuffer {
            val out = ByteArray(compressed.int)
            try {
                GZIPInputStream(ByteBufferInputStream(compressed)).use { `in` -> IO.readBytes(`in`, out) }
            } catch (e: IOException) {
                throw UncheckedIOException(e)
            }
            return ByteBuffer.wrap(out)
        }

        override fun compress(buf: ByteBuffer, dst: ByteBufferOutputStream) {
            dst.writeInt(buf.remaining())
            try {
                GZIPOutputStream(dst).use { out -> IO.transferTo(buf, out) }
            } catch (e: IOException) {
                throw UncheckedIOException(e)
            }
        }
    };

    abstract fun decompress(compressed: ByteBuffer): ByteBuffer

    abstract fun compress(buf: ByteBuffer, dst: ByteBufferOutputStream)

    fun compress(buf: ByteBuffer): ByteBuffer {
        val out = ByteBufferOutputStream()
        compress(buf, out)
        return out.buf.flip()
    }

    fun id(): Byte = ordinal.toByte()

    fun headerSize(): Int {
        return if (this === NONE) 0 else Integer.BYTES
    }

    companion object {
        fun of(id: Byte): Compressor {
            when (id) {
                0.toByte() -> return NONE
                1.toByte() -> return BZIP2
                2.toByte() -> return GZIP
            }
            throw IllegalArgumentException("" + id)
        }

        fun best(buf: ByteBuffer): Compressor {
            val out = ByteBufferOutputStream()
            GZIP.compress(buf.duplicate(), out)

            val gzip: Int = out.buf.position()
            out.buf.clear()
            BZIP2.compress(buf.duplicate(), out)

            val bzip2: Int = out.buf.position()
            val none = buf.remaining()

            if (none <= gzip && none <= bzip2) return NONE
            return if (gzip <= bzip2) GZIP else BZIP2
        }
    }
}

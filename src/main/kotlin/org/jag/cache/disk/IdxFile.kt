package org.jag.cache.disk

import org.jag.cache.util.IO
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

internal class IdxFile(file: Path?) : Closeable {
    private val channel: FileChannel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)
    private val buf = ByteBuffer.allocate(ENTRY_SIZE)
    @Throws(IOException::class)
    fun size(): Int {
        return (channel.size() / ENTRY_SIZE).toInt()
    }

    @Throws(IOException::class)
    fun read(group: Int): Entry? {
        val pos = group * ENTRY_SIZE
        if (pos + ENTRY_SIZE > channel.size()) return null
        channel.read(buf, pos.toLong())
        buf.clear()
        val length: Int = IO.getMedium(buf)
        val sector: Int = IO.getMedium(buf)
        buf.clear()
        return if (length <= 0 && sector == 0) null else Entry(length, sector)
    }

    @Throws(IOException::class)
    fun write(group: Int, length: Int, sector: Int) {
        IO.putMedium(buf, length)
        IO.putMedium(buf, sector)
        channel.write(buf.clear(), (group * ENTRY_SIZE).toLong())
        buf.clear()
    }

    @Throws(IOException::class)
    override fun close() {
        channel.close()
    }

    internal class Entry(val length: Int, val sector: Int)
    companion object {
        private const val ENTRY_SIZE = 6
    }

}

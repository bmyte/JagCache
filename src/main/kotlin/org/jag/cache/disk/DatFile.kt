package org.jag.cache.disk

import org.jag.cache.util.IO
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

internal class DatFile(file: Path?) : Closeable {
    private val channel: FileChannel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)
    private val buf = ByteBuffer.allocate(SECTOR_SIZE)
    @Throws(IOException::class)
    fun read(archive: Int, group: Int, length: Int, sector: Int): ByteBuffer {
        var secStart = sector
        val dst = ByteBuffer.allocate(length)
        var chunk = 0
        while (dst.hasRemaining()) {
            channel.read(buf, (secStart * SECTOR_SIZE).toLong())
            buf.clear()
            val sectorGroup = java.lang.Short.toUnsignedInt(buf.short)
            if (group != sectorGroup) throw IOException()
            val sectorChunk = java.lang.Short.toUnsignedInt(buf.short)
            if (chunk != sectorChunk) throw IOException()
            secStart = IO.getMedium(buf)
            val sectorArchive = java.lang.Byte.toUnsignedInt(buf.get())
            if (archive != sectorArchive) throw IOException()
            dst.put(IO.getSlice(buf, Math.min(buf.remaining(), dst.remaining())))
            buf.clear()
            chunk++
        }
        return dst.clear()
    }

    @Throws(IOException::class)
    fun append(archive: Int, group: Int, data: ByteBuffer): Int {
        val startSector = (channel.size() / SECTOR_SIZE).toInt()
        var sector = startSector
        var chunk = 0
        while (data.hasRemaining()) {
            buf.putShort(group.toShort())
            buf.putShort(chunk.toShort())
            IO.putMedium(buf, sector + 1)
            buf.put(archive.toByte())
            buf.put(IO.getSlice(data, Math.min(buf.remaining(), data.remaining())))
            channel.write(buf.clear(), (sector * SECTOR_SIZE).toLong())
            buf.clear()
            chunk++
            sector++
        }
        return startSector
    }

    @Throws(IOException::class)
    override fun close() {
        channel.close()
    }

    companion object {
        private const val SECTOR_SIZE = 520
    }

}
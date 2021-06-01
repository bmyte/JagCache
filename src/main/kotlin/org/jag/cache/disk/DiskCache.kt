package org.jag.cache.disk

import org.jag.cache.model.IndexMaster.Companion.MASTER_ARCHIVE
import org.jag.cache.model.LocalCache
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

class DiskCache private constructor(private val directory: Path) : LocalCache {
    private val datFile: DatFile = DatFile(directory.resolve(DAT_FILE_NAME))
    private val idxFiles: Array<IdxFile?> = arrayOfNulls<IdxFile>(MASTER_ARCHIVE + 1)
    @Throws(IOException::class)
    private fun getIdxFile(archive: Int): IdxFile {
        var f: IdxFile? = idxFiles[archive]
        if (f == null) {
            f = IdxFile(directory.resolve(IDX_FILE_NAME + archive))
            idxFiles[archive] = f
        }
        return f
    }

    @get:Synchronized
    override val archiveCount: Int
        get() = try {
            getIdxFile(MASTER_ARCHIVE).size()
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }

    @Synchronized
    override fun getGroupCompressed(archive: Int, group: Int): ByteBuffer? {
        return try {
            val e = getIdxFile(archive).read(group)
            if (e == null) null else datFile.read(archive, group, e.length, e.sector)
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }
    }

    @Synchronized
    override fun setGroupCompressed(archive: Int, group: Int, buf: ByteBuffer?) {
        try {
            getIdxFile(archive).write(group, buf!!.remaining(), datFile.append(archive, group, buf))
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }
    }

    @Synchronized
    @Throws(IOException::class)
    override fun close() {
        datFile.close()
        for (f in idxFiles) {
            f?.close()
        }
    }

    override fun toString(): String {
        return "DiskCache($directory)"
    }

    companion object {
        public const val DAT_FILE_NAME = "main_file_cache.dat2"
        public const val IDX_FILE_NAME = "main_file_cache.idx"
        @Throws(IOException::class)
        fun open(directory: Path): DiskCache {
            Files.createDirectories(directory)
            return DiskCache(directory)
        }
    }

}
package org.jag.cache.model

import org.jag.cache.model.IndexMaster.Companion.MASTER_ARCHIVE
import java.nio.ByteBuffer

interface LocalCache : AutoCloseable {
    fun getGroupCompressed(archive: Int, group: Int): ByteBuffer?
    fun getGroup(archive: Int, group: Int): Group? {
        return getGroup(archive, group, null)
    }

    fun getGroup(archive: Int, group: Int, key: IntArray?): Group? {
        val gc = getGroupCompressed(archive, group)
        return if (gc == null) null else Group.decompress(gc, key)
    }

    fun getIndex(archive: Int): Index? {
        val g = getGroup(MASTER_ARCHIVE, archive)
        return if (g == null) null else Index.decode(g.data)
    }

    fun setGroupCompressed(archive: Int, group: Int, buf: ByteBuffer?)
    val archiveCount: Int

}

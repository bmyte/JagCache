package org.jag.cache.model

import org.jag.cache.model.IndexMaster.Companion.MASTER_ARCHIVE
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

interface RemoteCache : AutoCloseable {
    fun getGroupCompressed(archive: Int, group: Int): CompletableFuture<ByteBuffer>?

    fun getGroup(archive: Int, group: Int): CompletableFuture<Group?> {
        return getGroup(archive, group, null)
    }

    fun getGroup(archive: Int, group: Int, key: IntArray?): CompletableFuture<Group?> {
        return getGroupCompressed(archive, group)!!.thenApply<Group> { gc: ByteBuffer? ->
            Group.decompress(gc!!, key)
        }
    }

    fun getMasterIndex(): CompletableFuture<Array<IndexMaster?>>? {
        return getGroup(MASTER_ARCHIVE, MASTER_ARCHIVE).thenApply { g: Group? ->
            IndexMaster.decodeAll(g!!.data)
        }
    }

    fun getIndex(archive: Int): CompletableFuture<Index>? {
        return getGroup(MASTER_ARCHIVE, archive).thenApply { g: Group? ->
            Index.decode(g!!.data)
        }
    }

}
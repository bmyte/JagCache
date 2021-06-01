package org.jag.cache

import net.spectral.logger.Logger
import org.jag.cache.disk.DiskCache.Companion.IDX_FILE_NAME
import org.jag.cache.model.Group
import org.jag.cache.model.Index
import org.jag.cache.model.IndexMaster.Companion.MASTER_ARCHIVE
import org.jag.cache.model.LocalCache
import org.jag.cache.model.RemoteCache
import org.jag.cache.util.IO
import java.util.ArrayList
import java.util.concurrent.CompletableFuture

object CacheUpdate {
    fun update(remote: RemoteCache, local: LocalCache): CompletableFuture<Void?>? {
        Logger.debug("retrieving master index from host...")
        return remote.getMasterIndex()!!.thenCompose { rm ->
            val fs = ArrayList<CompletableFuture<Void>>()
            for (i in rm.indices) {
                val rim = rm[i]
                val lig = local.getGroup(MASTER_ARCHIVE, i)
                Logger.debug("checking $IDX_FILE_NAME$i")
                val li = if (lig == null) null else Index.decode(lig.data)
                if (li == null || rim?.crc32 != lig?.crc32 || rim?.version != li.version) {
                    Logger.debug("updating $IDX_FILE_NAME$i")
                    fs.add(updateArchive(remote, local, i, li))
                }
            }
            IO.allOf(fs)
        }
    }

    fun updateArchive(remote: RemoteCache, local: LocalCache, archive: Int): CompletableFuture<Void> {
        return updateArchive(remote, local, archive, local.getIndex(archive)!!)
    }

    private fun updateArchive(
        remote: RemoteCache,
        local: LocalCache,
        archive: Int,
        li: Index?
    ): CompletableFuture<Void> {
        return remote.getGroupCompressed(MASTER_ARCHIVE, archive)!!.thenCompose { igc ->
                local.setGroupCompressed(MASTER_ARCHIVE, archive, igc.duplicate())
                updateArchive0(remote, local, archive, Index.decode(Group.decompress(igc, null).data), li)
            }
    }

    private fun updateArchive0(
        remote: RemoteCache,
        local: LocalCache,
        archive: Int,
        ri: Index,
        li: Index?
    ): CompletableFuture<Void> {
        val fs = ArrayList<CompletableFuture<Void>>()
        var lj = 0
        for (rg in ri.groups) {
            var lg: Index.Group? = null
            while (li != null && lj < li.groups.size) {
                val g= li.groups[lj++]
                if (rg.id == g.id) {
                    lg = g
                    break
                } else if (rg.id < g.id) {
                    lj--
                    break
                }
            }
            if (lg == null || rg.crc32 != lg.crc32 || rg.version != lg.version) {
                fs.add(remote.getGroupCompressed(archive, rg.id)!!.thenAccept { gc ->
                    local.setGroupCompressed(archive, rg.id, gc)
                })
            }
        }
        return IO.allOf(fs)
    }
}

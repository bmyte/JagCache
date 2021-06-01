package org.jag.cache.model

import org.jag.cache.util.IO
import java.nio.ByteBuffer
import java.nio.ShortBuffer
import java.util.*

class Index(val version: Int, val groups: Array<Group>) {

    override fun toString(): String {
        return "Index(version=" + version + ", groups=${groups.contentToString()})"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Index) return false
        return version == other.version && groups.contentEquals(other.groups)
    }

    override fun hashCode(): Int {
        return version xor groups.contentHashCode()
    }

    fun encode(): ByteBuffer {
        val hasNames = hasNames()
        var len = 1 + 4 + 1 + 2 + groups.size * (2 + (if (hasNames) 4 else 0) + 4 + 4 + 2)
        for (g in groups) {
            len += g.files.size * (2 + if (hasNames) 4 else 0)
        }
        val buf = ByteBuffer.allocate(len)
        buf.put(6.toByte())
        buf.putInt(version)
        buf.put((if (hasNames) 1 else 0).toByte())
        buf.putShort(groups.size.toShort())
        var lastGroup = 0
        for (g in groups) {
            buf.putShort((g.id - lastGroup).toShort())
            lastGroup = g.id
        }
        if (hasNames) {
            for (g in groups) {
                buf.putInt(g.nameHash)
            }
        }
        for (g in groups) {
            buf.putInt(g.crc32)
        }
        for (g in groups) {
            buf.putInt(g.version)
        }
        for (g in groups) {
            buf.putShort(g.files.size.toShort())
        }
        for (g in groups) {
            var lastFile = 0
            for (f in g.files) {
                buf.putShort((f.id - lastFile).toShort())
                lastFile = f.id
            }
        }
        if (hasNames) {
            for (g in groups) {
                for (f in g.files) {
                    buf.putInt(f.nameHash)
                }
            }
        }
        return buf.flip()
    }

    private fun hasNames(): Boolean {
        for (g in groups) {
            if (g.nameHash != 0) return true
            for (f in g.files) {
                if (f.nameHash != 0) return true
            }
        }
        return false
    }

    class Group(val id: Int, val nameHash: Int, val crc32: Int, val version: Int, val files: Array<File>) {

        override fun toString(): String {
            return "Group(id=$id, nameHash=$nameHash, crc32=$crc32, version=$version, files=${files.contentToString()})"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Group) return false
            return id == other.id && nameHash == other.nameHash && crc32 == other.crc32 && version == other.version && files.contentEquals(other.files)
        }

        override fun hashCode(): Int {
            return crc32
        }

        companion object {
            fun split(group: ByteBuffer, fileCount: Int): Array<ByteBuffer?> {
                Objects.requireNonNull(group)
                if (fileCount == 1) return arrayOf(group)
                val fs = arrayOfNulls<ByteBuffer>(fileCount)
                val chunkCount = java.lang.Byte.toUnsignedInt(group[group.limit() - 1])
                val chunks = Array(fileCount) {
                    arrayOfNulls<ByteBuffer>(
                        chunkCount
                    )
                }
                val chunkSizes = group.duplicate().position(group.limit() - 1 - chunkCount * fileCount * Integer.BYTES)
                for (c in 0 until chunkCount) {
                    var chunkSize = 0
                    for (f in 0 until fileCount) {
                        chunks[f][c] = IO.getSlice(group, chunkSizes.int.let { chunkSize += it; chunkSize })
                    }
                }
                group.position(group.limit())
                for (f in 0 until fileCount) {
                    fs[f] = IO.join(*chunks[f])
                }
                return fs
            }

            fun merge(files: Array<ByteBuffer>): ByteBuffer {
                if (files.size == 1) return files[0]
                var len = 1
                for (f in files) {
                    len += Integer.BYTES + f.remaining()
                }
                val dst = ByteBuffer.allocate(len)
                val sizes = dst.duplicate().position(dst.limit() - 1 - files.size * Integer.BYTES)
                var lastFileSize = 0
                for (f in files) {
                    sizes.putInt(f.remaining() - lastFileSize)
                    lastFileSize = f.remaining()
                    dst.put(f.duplicate())
                }
                dst.put(dst.limit() - 1, 1.toByte())
                return dst.rewind()
            }
        }

    }

    class File(val id: Int, val nameHash: Int) {
        override fun toString(): String {
            return "File(id=$id, nameHash=$nameHash)"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is File) return false
            return id == other.id && nameHash == other.nameHash
        }

        override fun hashCode(): Int {
            return id xor nameHash
        }
    }

    companion object {
        fun decode(buf: ByteBuffer): Index {
            val protocol = buf.get()
            require(!(protocol.toInt() != 5 && protocol.toInt() != 6))
            val version = if (protocol >= 6) buf.int else 0
            val hasNames = buf.get().toInt() != 0
            val groupCount = java.lang.Short.toUnsignedInt(buf.short)
            val groupIds= IO.getShortSlice(buf, groupCount)
            val groupNameHashes= if (hasNames) IO.getIntSlice(buf, groupCount) else null
            val groupCrcs= IO.getIntSlice(buf, groupCount)
            val groupVersions= IO.getIntSlice(buf, groupCount)
            val fileCounts= IO.getShortSlice(buf, groupCount)
            val fileIds = arrayOfNulls<ShortBuffer>(groupCount)
            for (a in 0 until groupCount) {
                fileIds[a] = IO.getShortSlice(buf, java.lang.Short.toUnsignedInt(fileCounts.get(a)))
            }
            val groups = mutableListOf<Group>()
            var groupId = 0
            for (a in 0 until groupCount) {
                val fileCount = java.lang.Short.toUnsignedInt(fileCounts.get(a))
                val files = mutableListOf<File>()
                var fileId = 0
                for (f in 0 until fileCount) {
                    val fileNameHash = if (hasNames) buf.int else 0
                    fileId += fileIds[a]!![f].toInt()
                    files.add(f, File(fileId, fileNameHash))
                }
                val groupNameHash = if (hasNames) groupNameHashes!!.get(a) else 0
                groupId += groupIds.get(a)
                groups.add(a, Group(groupId, groupNameHash, groupCrcs.get(a), groupVersions.get(a), files.toTypedArray()))
            }
            return Index(version, groups.toTypedArray())
        }
    }

}

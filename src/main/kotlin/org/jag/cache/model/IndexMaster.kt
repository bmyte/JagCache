package org.jag.cache.model

import java.nio.ByteBuffer

class IndexMaster private constructor(val crc32: Int, val version: Int) {
    override fun toString(): String {
        return "IndexMaster(crc32=$crc32, version=$version)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexMaster) return false
        return crc32 == other.crc32 && version == other.version
    }

    override fun hashCode(): Int {
        return crc32
    }

    companion object {
        val MASTER_ARCHIVE: Int = 255

        fun decodeAll(masterIndex: ByteBuffer): Array<IndexMaster?> {
            val count = masterIndex.remaining() / (Integer.BYTES * 2)
            val mi = arrayOfNulls<IndexMaster>(count)
            for (i in 0 until count) {
                mi[i] = IndexMaster(masterIndex.int, masterIndex.int)
            }
            return mi
        }
    }
}
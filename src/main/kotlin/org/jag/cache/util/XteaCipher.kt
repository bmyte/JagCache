package org.jag.cache.util

import java.nio.ByteBuffer
import java.util.*

object XteaCipher {
    const val KEY_SIZE = 4
    private const val PHI = -0x61c88647
    private const val ROUNDS = 32
    fun encrypt(buf: ByteBuffer, key: IntArray) {
        Objects.requireNonNull(buf)
        if (isKeyEmpty(key)) return
        var i = buf.position()
        while (i <= buf.limit() - Integer.BYTES * 2) {
            var v0 = buf.getInt(i)
            var v1 = buf.getInt(i + Integer.BYTES)
            var sum = 0
            for (r in 0 until ROUNDS) {
                v0 += (v1 shl 4 xor (v1 ushr 5)) + v1 xor sum + key[sum and 3]
                sum += PHI
                v1 += (v0 shl 4 xor (v0 ushr 5)) + v0 xor sum + key[sum ushr 11 and 3]
            }
            buf.putInt(i, v0)
            buf.putInt(i + Integer.BYTES, v1)
            i += Integer.BYTES * 2
        }
    }

    @Suppress("INTEGER_OVERFLOW")
    fun decrypt(buf: ByteBuffer, key: IntArray) {
        Objects.requireNonNull(buf)
        if (isKeyEmpty(key)) return
        var i = buf.position()
        while (i <= buf.limit() - Integer.BYTES * 2) {
            var v0 = buf.getInt(i)
            var v1 = buf.getInt(i + Integer.BYTES)
            var sum = PHI * ROUNDS
            for (r in 0 until ROUNDS) {
                v1 -= (v0 shl 4 xor (v0 ushr 5)) + v0 xor sum + key[sum ushr 11 and 3]
                sum -= PHI
                v0 -= (v1 shl 4 xor (v1 ushr 5)) + v1 xor sum + key[sum and 3]
            }
            buf.putInt(i, v0)
            buf.putInt(i + Integer.BYTES, v1)
            i += Integer.BYTES * 2
        }
    }

    private fun isKeyEmpty(key: IntArray): Boolean {
        require(key.size == KEY_SIZE)
        for (i in 0 until KEY_SIZE) {
            if (key[i] != 0) return false
        }
        return true
    }
}
package org.jag.cache.model

import net.spectral.logger.Logger
import org.jag.cache.model.IndexMaster.Companion.MASTER_ARCHIVE
import org.jag.cache.util.IO
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.lang.IllegalArgumentException
import java.net.Socket
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.*

class NetCache private constructor(socket: Socket) : RemoteCache {
    private val pendingWrites: BlockingQueue<Request> = LinkedBlockingQueue()
    private val pendingReads: BlockingQueue<Request> = LinkedBlockingQueue(MAX_REQS)
    private var readThread: Thread = Thread()
    private var writeThread: Thread = Thread()

    @Throws(InterruptedException::class, IOException::class)
    private fun read(`in`: InputStream) {
        val headerBuf = ByteBuffer.allocate(HEADER_SIZE)
        while (true) {
            val req = pendingReads.take()
            if (req.isShutdownSentinel) return
            IO.readBytes(`in`, headerBuf.array())
            val archive = headerBuf.get()
            val group = headerBuf.short
            val compressor= Compressor.of(headerBuf.get())
            val compressedSize = headerBuf.int
            headerBuf.clear()
            if (archive != req.archive || group != req.group) throw IOException()
            val resSize: Int = HEADER_SIZE + compressedSize + compressor.headerSize()
            val resArray = Arrays.copyOf(headerBuf.array(), resSize)
            if (resSize <= WINDOW_SIZE) {
                IO.readNBytes(`in`, resArray, HEADER_SIZE, resSize - HEADER_SIZE)
            } else {
                IO.readNBytes(`in`, resArray, HEADER_SIZE, WINDOW_SIZE - HEADER_SIZE + 1)
                var pos = WINDOW_SIZE
                while (true) {
                    if (resArray[pos] != WINDOW_DELIMITER) throw IOException()
                    pos += if (resSize - pos >= WINDOW_SIZE) {
                        IO.readNBytes(`in`, resArray, pos, WINDOW_SIZE)
                        WINDOW_SIZE - 1
                    } else {
                        IO.readNBytes(`in`, resArray, pos, resSize - pos)
                        break
                    }
                }
            }
            val res = ByteBuffer.wrap(resArray).position(3)
            req.future!!.completeAsync { res }
        }
    }

    @Throws(InterruptedException::class, IOException::class)
    private fun write(out: OutputStream) {
        val writeBuf = ByteBuffer.allocate(4)
        while (true) {
            val req = pendingWrites.take()
            if (req.isShutdownSentinel) {
                pendingReads.put(req)
                return
            }
            req.writeTo(writeBuf.clear())
            out.write(writeBuf.array())
            pendingReads.put(req)
        }
    }

    override fun getGroupCompressed(archive: Int, group: Int): CompletableFuture<ByteBuffer>? {
        val req = Request(archive.toByte(), group.toShort())
        pendingWrites.add(req)
        return req.future
    }

    override fun close() {
        pendingWrites.add(Request.shutdownSentinel())
    }

    private class Request {
        val archive: Byte
        val group: Short
        val future: CompletableFuture<ByteBuffer>?

        constructor(archive: Byte, group: Short) {
            this.archive = archive
            this.group = group
            future = CompletableFuture()
        }

        private val isUrgent: Boolean get() = archive==MASTER_ARCHIVE.toByte()

        fun writeTo(buf: ByteBuffer) {
            buf.put((if (isUrgent) 1 else 0).toByte()).put(archive).putShort(group)
        }

        private constructor() {
            archive = -1
            group = -1
            future = null
        }

        val isShutdownSentinel: Boolean
            get() = future == null

        companion object {
            fun shutdownSentinel(): Request {
                return Request()
            }
        }
    }

    companion object {
        const val DEFAULT_PORT = 43594
        private const val MAX_REQS = 19
        private const val WINDOW_SIZE = 512
        private const val HEADER_SIZE = 8
        private const val WINDOW_DELIMITER: Byte = -1

        @Throws(IOException::class)
        fun connect(
            address: SocketAddress,
            revision: Int
        ): NetCache {
            val socket = Socket()
            try {
                connect(socket, address, revision)
            } catch (e: IOException) {
                IO.closeQuietly(e, socket)
                throw e
            }
            return NetCache(socket)
        }

        @Throws(IOException::class)
        private fun connect(socket: Socket, address: SocketAddress, revision: Int) {
            socket.tcpNoDelay = true
            socket.soTimeout = 30000
            socket.connect(address)
            Logger.info("attempting sync with specified revision ($revision)...")
            socket.getOutputStream().write(ByteBuffer.allocate(5).put(15.toByte()).putInt(revision).array())
            if (socket.getInputStream().read() != 0) {
                val err = IllegalArgumentException("invalid revision specified for the server!")
                Logger.error(err)
                throw err
            }
            Logger.info("revision correct!")
            socket.getOutputStream()
                .write(ByteBuffer.allocate(4).put(3.toByte()).put(0.toByte()).putShort(0.toShort()).array())
        }
    }

    init {
        val threadFactory = Executors.defaultThreadFactory()
        readThread = threadFactory.newThread {
            try {
                read(socket.getInputStream())
                try {
                    socket.close()
                } catch (e: IOException) {
                    e.printStackTrace()
                }
            } catch (e: IOException) {
                writeThread.interrupt()
                IO.closeQuietly(e, socket)
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
        writeThread = threadFactory.newThread {
            try {
                write(socket.getOutputStream())
            } catch (e: IOException) {
                readThread.interrupt()
                IO.closeQuietly(e, socket)
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
        readThread.start()
        writeThread.start()
    }
}
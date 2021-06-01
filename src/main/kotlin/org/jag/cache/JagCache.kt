import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import net.spectral.logger.Logger
import org.jag.cache.CacheUpdate
import org.jag.cache.disk.DiskCache
import org.jag.cache.model.NetCache
import java.net.InetSocketAddress
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

class JagCache : CliktCommand() {
    private val host: String by option(help="the internet address of the cache server").default("oldschool7.runescape.com")
    private val revision: Int by option(help = "the revision expected by server").int().default(196)

    private val cacheDir: String by option(help="the location to save the cache").default(".cache")

    override fun run() {
        Logger.info("syncing cache with remote host $host...")
        val start = Instant.now()
        NetCache.connect(InetSocketAddress(host, NetCache.DEFAULT_PORT), revision).use { net ->
            DiskCache.open(Path.of(cacheDir)).use { disk ->
                CacheUpdate.update(net, disk)?.join()
            }
        }
        Logger.info("cache update completed in ${Duration.between(start, Instant.now()).seconds} seconds")
    }
}

fun main(args: Array<String>) = JagCache().main(args)

package no.nav.sf.pdl

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.enableNAISAPI

private const val EV_bootstrapWaitTime = "MS_BETWEEN_WORK" // default to 10 minutes
private val bootstrapWaitTime = AnEnvironment.getEnvOrDefault(EV_bootstrapWaitTime, "60000").toLong()

private const val number_of_eventless_worksessions_before_termination = 1000000000 // Restart now and then if not consuming records to load rotating kafka certificates

/**
 * Bootstrap is a very simple µService manager
 * - start, enables mandatory NAIS API before entering 'work' loop
 * - loop,  invokes a work -, then a wait session, until shutdown - or prestop hook (NAIS feature),
 * - conditionalWait, waits a certain time, checking the hooks once a while
 */

object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ws: WorkSettings = WorkSettings()) {
        enableNAISAPI {
            log.info { "Starting - grace period 3 m after enableNAISAPI" }
            conditionalWait(180000)
            log.info { "Starting - post grace period enableNAISAPI" }
            gtTest(ws)
            // initLoadTest(ws)
            // initLoad(ws)
            loadPersonCache(ws) // Will carry cache in memory after this point
            loop(ws)
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop(ws: WorkSettings) {
        if (numberOfWorkSessionsWithoutEvents >= number_of_eventless_worksessions_before_termination) log.info { "Will terminate (and reboot) since number of successive work sessions without consumed events is at least $number_of_eventless_worksessions_before_termination" }
        val stop = ShutdownHook.isActive() || PrestopHook.isActive() || (numberOfWorkSessionsWithoutEvents >= number_of_eventless_worksessions_before_termination)
        when {
            stop -> Unit
            !stop -> {
                loop(work(ws).first
                        .also { conditionalWait() })
            }
        }
    }

    fun conditionalWait(ms: Long = bootstrapWaitTime) =
            runBlocking {
                log.info { "Will wait $ms ms" }

                val cr = launch {
                    runCatching { delay(ms) }
                            .onSuccess { log.info { "waiting completed" } }
                            .onFailure { log.info { "waiting interrupted" } }
                }

                tailrec suspend fun loop(): Unit = when {
                    cr.isCompleted -> Unit
                    ShutdownHook.isActive() || PrestopHook.isActive() -> cr.cancel()
                    else -> {
                        delay(250L)
                        loop()
                    }
                }

                loop()
                cr.join()
            }
}

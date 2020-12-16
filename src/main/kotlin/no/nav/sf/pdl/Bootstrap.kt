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

/**
 * Bootstrap is a very simple µService manager
 * - start, enables mandatory NAIS API before entering 'work' loop
 * - loop,  invokes a work -, then a wait session, until shutdown - or prestop hook (NAIS feature),
 * - conditionalWait, waits a certain time, checking the hooks once a while
 */
object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start() {
        enableNAISAPI {
            log.info { "Starting - grace period 3 m after enableNAISAPI" }
            conditionalWait(180000)
            log.info { "Starting - post grace period enableNAISAPI" }
            workMetrics.busy.set(1.0)
            // gtInitLoad() // Publish to cache topic also load cache in app (no need to to do loadGtCache)
            loadGtCache() // Use this if not gt init load is used
            // initLoadTest() //Investigate run of number of records on topic if suspecting drop of records in init run
            // initLoad() // Only publish to person/cache topic
            loadPersonCache() // Will carry cache in memory after this point
            loop()
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop() {
    val stop = ShutdownHook.isActive() || PrestopHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                workMetrics.busy.set(1.0)
                val isOK = work().isOK()
                workMetrics.busy.set(0.0)
                conditionalWait()
                if (isOK) loop() else log.info { "Terminate signal (Work exit reason NOK)" }.also { conditionalWait() }
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

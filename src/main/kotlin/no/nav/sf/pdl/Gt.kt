package no.nav.sf.pdl

import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.sf.library.jsonNonStrict

private val log = KotlinLogging.logger {}

sealed class GtBase
object InvalidGt : GtBase()

fun String.getGtFromJson(): GtBase = runCatching {
    jsonNonStrict.parse(Gt.serializer(), this)
}
        .onFailure {
            log.error { "Cannot convert kafka value to gt - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidGt)

@Serializable
data class Gt(
    val geografiskTilknytning: HentePerson.GeografiskTilknytning
) : GtBase()

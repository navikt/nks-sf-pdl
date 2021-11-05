import java.io.File
import mu.KotlinLogging
import no.nav.sf.pdl.PersonProtobufIssue
import no.nav.sf.pdl.PersonSf
import no.nav.sf.pdl.loadGtCache
import no.nav.sf.pdl.loadPersonCache
import no.nav.sf.pdl.personCache
import no.nav.sf.pdl.toPersonSf

private val log = KotlinLogging.logger {}

internal fun investigateCache() {

    val resultListPdlQueue: MutableList<Pair<String, ByteArray?>> = mutableListOf()
    val offsetCutOffStart = 0L // 250_000_000L
    val offsetCutOffEnd = -1L

    log.info { "INVESTIGATE - Will load gt Cache" }

    loadGtCache() // For processing old queue to cache equivalent

    log.info { "INVESTIGATE - Will load person Cache" }

    loadPersonCache()
/*
    log.info { "INVESTIGATE - Will start consume pdl queue for cache compare" }
    var count = 0
    var retries = 5
    workMetrics.testRunRecordsParsed.clear()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerOnPremSeparateClientId,
            fromBeginning = true,
            topics = listOf(kafkaPDLTopic)
    )

    kafkaConsumerPdlTest.consume { cRecords ->
        if (cRecords.isEmpty) {
            if (count < 2 && retries > 0) {
                log.info { "Init test run: Did not get any messages on retry $retries, will wait 60 s and try again" }
                retries--
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            } else {
                log.info { "Init test run: Decided no more events on topic" }
                return@consume KafkaConsumerStates.IsFinished
            }
        }

        if (offsetCutOffStart != -1L && cRecords.last().offset() < offsetCutOffStart) {
            // Not yet reached interesting offset range - will ignore
            return@consume KafkaConsumerStates.IsOk
        }

        if (offsetCutOffEnd != -1L && cRecords.first().offset() > offsetCutOffEnd) {
            log.info { "INVESTIGATE - passed interesting offset range. Finish read job" }
            return@consume KafkaConsumerStates.IsFinished
        }

        count += cRecords.count()

        workMetrics.testRunRecordsParsed.inc(cRecords.count().toDouble())

        // ************* DO THE SAME AS WORK BUT PUT RESULT IN LIST

        val results = cRecords.map { cr ->
            if (cr.value() == null) {
                val personTombestone = PersonTombestone(aktoerId = cr.key())
                workMetrics.tombstones.inc()
                Triple(KafkaConsumerStates.IsOk, personTombestone, cr.offset())
            } else {
                when (val query = cr.value()!!.getQueryFromJson()) {
                    InvalidQuery -> {
                        workMetrics.consumerIssues.inc()
                        log.error { "Unable to parse topic value PDL" }
                        Triple(KafkaConsumerStates.HasIssues, PersonInvalid, cr.offset())
                    }
                    is Query -> {
                        when (val personSf = query.toPersonSf()) {
                            is PersonSf -> {
                                // Enrich with data from gt Cache
                                if (gtCache.containsKey(personSf.aktoerId)) {
                                    workMetrics.enriching_from_gt_cache.inc()
                                    val enriched = if (gtCache[personSf.aktoerId] == null) {
                                        personSf.copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                                    } else {
                                        val gtBase = GtBaseFromProto(keyAsByteArray(personSf.aktoerId), gtCache[personSf.aktoerId])
                                        if (gtBase is GtValue) {
                                            personSf.copy(kommunenummerFraGt = gtBase.kommunenummerFraGt, bydelsnummerFraGt = gtBase.bydelsnummerFraGt)
                                        } else {
                                            workMetrics.consumerIssues.inc()
                                            log.error { "Fail to parse gt from gt cache" }
                                            personSf
                                        }
                                    }
                                    Triple(KafkaConsumerStates.IsOk, enriched, cr.offset())
                                } else {
                                    Triple(KafkaConsumerStates.IsOk, personSf, cr.offset())
                                }
                            }
                            is PersonInvalid -> {
                                workMetrics.consumerIssues.inc()
                                Triple(KafkaConsumerStates.HasIssues, PersonInvalid, cr.offset())
                            }
                            else -> {
                                workMetrics.consumerIssues.inc()
                                log.error { "Returned unhandled PersonBase from Query.toPersonSf" }
                                Triple(KafkaConsumerStates.HasIssues, PersonInvalid, cr.offset())
                            }
                        }
                    }
                }
            }
        }
        val areOk = results.map { it.first }.filterIsInstance<KafkaConsumerStates.HasIssues>().isEmpty()

        if (areOk) {
            results.filter {
                when (val personBase = it.second) {
                    is PersonTombestone -> {
                        if (!personCache.containsKey(personBase.aktoerId)) {
                            workMetrics.cache_new_tombstone.inc()
                            personCache[personBase.aktoerId] = null
                            true
                        } else if (personCache[personBase.aktoerId] != null) {
                            workMetrics.cache_update_tombstone.inc()
                            personCache[personBase.aktoerId] = null
                            true
                        } else {
                            workMetrics.cache_blocked_tombstone.inc()
                            false
                        }
                    }
                    is PersonSf -> {
                        if (!personCache.containsKey(personBase.aktoerId)) {
                            workMetrics.cache_new.inc()
                            personCache[personBase.aktoerId] = personBase.toPersonProto().second.toByteArray()
                            true
                        } else if (personCache[personBase.aktoerId] == null || personCache[personBase.aktoerId]?.contentEquals(personBase.toPersonProto().second.toByteArray()) != true) {
                            workMetrics.cache_update.inc()
                            personCache[personBase.aktoerId] = personBase.toPersonProto().second.toByteArray()
                            true
                        } else {
                            workMetrics.cache_blocked.inc()
                            false
                        }
                    }
                    else -> return@consume KafkaConsumerStates.HasIssues
                }
            }.filter {
                val hollowState = (it.second is PersonSf) && (it.second as PersonSf).isHollowState()
                if (hollowState) {
                    // Investigate.writeText("${(it.second as PersonSf).aktoerId} SKIP ENTRY THAT IS HOLLOW STATE", true)
                }
                !hollowState
            }.map {
                when (val personBase = it.second) {
                    is PersonTombestone -> {
                        Triple<PersonProto.PersonKey, PersonProto.PersonValue?, Long>(personBase.toPersonTombstoneProtoKey(), null, it.third)
                    }
                    is PersonSf -> {
                        val protoPair = personBase.toPersonProto()
                        Triple(protoPair.first, protoPair.second, it.third)
                    }
                    else -> return@consume KafkaConsumerStates.HasIssues
                }
            }.fold(true) { acc, triple ->
                acc && triple.second.let {
                    resultListPdlQueue.add(Pair(triple.first.aktoerId, triple.second?.toByteArray()))
                    true
                }
            }.let { sent ->
                when (sent) {
                    true -> KafkaConsumerStates.IsOk
                    false -> KafkaConsumerStates.HasIssues.also {
                        log.error { "Should not reach!" }
                    }
                }
            }
        } else {
            log.error { "Issue found" }
            KafkaConsumerStates.HasIssues
        }
        // *************

        KafkaConsumerStates.IsOk
    }
    heartBeatConsumer = 0

    log.info { "INVESTIGATE pdl Cache - Number of records from topic $kafkaPersonTopic is ${resultListPdlQueue.size}" }

    pdlQueueCache.putAll(resultListPdlQueue.toMap())

    log.info { "INVESTIGATE pdl Cache - resulting cache size ${pdlQueueCache.size} of which are tombstones ${pdlQueueCache.values.filter{it == null}.count()}" }

    resultListPdlQueue.clear()

    var notReflected = 0 // expected 0
    var uptodate = 0
    var mismatch = 0

    var parseerror = 0

    var uptodatebytes = 0

    var tombstoneskip = 0

    var heaviercompare = 100

    pdlQueueCache.forEach {
        if (!personCache.contains(it.key)) {
            notReflected++
        } else if (personCache[it.key] == null || it.value == null) {
            tombstoneskip++
        } else {
            // if ((it.first as PersonSf).identer.any { it.ident == targetAktoerId || it.ident == targetfnr2 } || (it.first as PersonSf).folkeregisteridentifikator.any { it.identifikasjonsnummer == targetfnr2 })
            if (it.key == "1000033414297") {
                log.info { "INVESTIGATE - hit interesting" }
                // heaviercompare--
                val parsedPersonCache = personCache[it.key]!!.toPersonSf(it.key)
                val parsedPdlCache = it.value!!.toPersonSf(it.key)
                if (parsedPersonCache is PersonProtobufIssue || parsedPdlCache is PersonProtobufIssue) {
                    log.error { "INVESTIGATE - Parse error" }
                    parseerror++
                } else {
                    val personCacheJson = (parsedPersonCache as PersonSf).toJson()
                    val pdlCacheJson = (parsedPdlCache as PersonSf).toJson()
                    val match = personCacheJson == pdlCacheJson
                    val filename = if (match) "hit" else "miss"
                    File("/tmp/$filename").writeText("Key: ${it.key}\nPersonCache:\n$personCacheJson\n\nPdlCache:\n$pdlCacheJson\n\nBytecompare:${personCache[it.key]!!.contentEquals(it.value!!)}")
                    if (match) {
                        uptodate++
                    } else {
                        mismatch++
                    }
                }
            } else if (personCache[it.key]!!.contentEquals(it.value!!)) {
                uptodate++
                uptodatebytes++
            } else {
                mismatch++

                if (mismatch < 1000) {
                    File("/tmp/mismatch").appendText("${it.key}\n")
                }
            }
        }
    }

    File("/tmp/mismatch").appendText("$mismatch number of mismatches")

 */

    // log.info { "INVESTIGATE. Of ${pdlQueueCache.size} aktoers investigated, $tombstoneskip are skipped due tombstone, $parseerror parse errors, $mismatch are mismatch, $uptodate is up-to-date ($uptodatebytes by bytecompare), and $notReflected is not reflected in person cache" }

    // pdlQueueCache.clear()

    var parseerror = 0

    listOf("1000049973123", "1000033414297", "1000016199761").forEach { aktoerid ->
        val parsedPersonCache = personCache[aktoerid]!!.toPersonSf(aktoerid)
        if (parsedPersonCache is PersonProtobufIssue) {
            log.error { "INVESTIGATE - short check Parse error" }
            parseerror++
        } else {
            log.error { "INVESTIGATE - short check hit" }
            val personCacheJson = (parsedPersonCache as PersonSf).toJson()
            File("/tmp/$aktoerid").writeText("Key: ${aktoerid}\nPersonCache:\n$personCacheJson\n\n")
        }
    }

    log.info { "INVESTIGATE - done Investigate cache short check ,parseerror $parseerror" } // , Total records investigated from topic: ${workMetrics.testRunRecordsParsed.get().toInt()}" }
}

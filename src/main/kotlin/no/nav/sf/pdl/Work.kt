package no.nav.sf.pdl

import java.io.File
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.currentConsumerMessageHost
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
const val EV_kafkaConsumerTopicGt = "KAFKA_TOPIC_GT"
const val EV_kafkaProducerTopicGt = "KAFKA_PRODUCER_TOPIC_GT"
const val EV_kafkaSchemaReg = "KAFKA_SCREG"
const val EV_kafkaBrokersOnPrem = "KAFKA_BROKERS_ON_PREM"

// Environment dependencies injected in pod by kafka solution
const val EV_kafkaKeystorePath = "KAFKA_KEYSTORE_PATH"
const val EV_kafkaCredstorePassword = "KAFKA_CREDSTORE_PASSWORD"
const val EV_kafkaTruststorePath = "KAFKA_TRUSTSTORE_PATH"

val kafkaSchemaReg = AnEnvironment.getEnvOrDefault(EV_kafkaSchemaReg, "http://localhost:8081")
val kafkaPersonTopic = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopic, "$PROGNAME-producer")
val kafkaPDLTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")
val kafkaGTTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopicGt, "$PROGNAME-consumer-gt")
val kafkaProducerTopicGt = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopicGt, "$PROGNAME-producer-gt")

fun fetchEnv(env: String): String {
    return AnEnvironment.getEnvOrDefault(env, "$env missing")
}

val workMetrics = WMetrics()
val ws = WorkSettings()

sealed class ExitReason {
    object NoKafkaProducer : ExitReason()
    object NoKafkaConsumer : ExitReason()
    object KafkaIssues : ExitReason()
    object NoEvents : ExitReason()
    object NoCache : ExitReason()
    object InvalidCache : ExitReason()
    object Work : ExitReason()

    fun isOK(): Boolean = this is Work || this is NoEvents
}

var firstOffsetLimitGt = -1L

internal fun updateGtCacheAndAffectedPersons(): ExitReason {
    var gtRetries = 5

    var exitReason: ExitReason = ExitReason.NoKafkaConsumer

    val skipUpdate: MutableSet<String> = mutableSetOf()

    var first = true
    var firstPersonUpdate = true
    var lastOffset = 0L

    val resultListChangesToGTCache: MutableList<Pair<String, ByteArray?>> = mutableListOf()

    currentConsumerMessageHost = "GT_ONPREM"
    AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerOnPremReducedPollSize,
            fromBeginning = false,
            topics = listOf(kafkaGTTopic)
    ).consume { consumerRecords ->
        if (consumerRecords.isEmpty) {
            if (workMetrics.gtRecordsParsed.get().toInt() == 0 && gtRetries > 0) {
                exitReason = ExitReason.NoEvents
                gtRetries--
                log.info { "Work Gt - retry connection after waiting 60 s. Retries left: $gtRetries" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        exitReason = ExitReason.Work

        if (consumerRecords.last().offset() < firstOffsetLimitGt) {
            log.error { "Gt attempting consuming offset last ${consumerRecords.last().offset()} before limit $firstOffsetLimitGt. Failed to fetch offset commit from kafka? abort" }
            exitReason = ExitReason.KafkaIssues
            return@consume KafkaConsumerStates.HasIssues
        }

        workMetrics.gtRecordsParsed.inc(consumerRecords.count().toDouble())
        consumerRecords.forEach {
            if (first) {
                first = false
                log.info { "Work gt cache update First offset read ${it.offset()}" }
            }
            lastOffset = it.offset()
            // Investigate.writeText("CONSUMED GT OFFSET ${it.offset()}", true)
            if (it.value() == null) {
                if (gtCache.containsKey(it.key()) && gtCache[it.key()] == null) {
                    workMetrics.gt_cache_blocked_tombstone.inc()
                } else {
                    if (gtCache.containsKey(it.key())) workMetrics.gt_cache_update_tombstone.inc() else workMetrics.gt_cache_new_tombstone.inc()
                    // Investigate.writeText("${it.key()} DETECTED GT VALUE FROM ${gtCache[it.key()]} TO NULL", true)
                    gtCache[it.key()] = null
                    resultListChangesToGTCache.add(Pair(it.key(), null))
                }
            } else {
                it.value()?.let { v ->
                    when (val gt = v.getGtFromJson()) {
                        is Gt -> {
                            when (val gtValue = gt.toGtValue(it.key())) {
                                is GtValue -> {
                                    val gtAsBytes = gtValue.toGtProto().second.toByteArray()
                                    if (gtCache.containsKey(it.key()) && gtCache[it.key()]?.contentEquals(gtAsBytes) == true) {
                                        workMetrics.gt_cache_blocked.inc()
                                    } else {
                                        if (gtCache.containsKey(it.key())) workMetrics.gt_cache_update.inc() else workMetrics.gt_cache_new.inc()
                                        if (!gtCache.containsKey(it.key())) {
                                            // Investigate.writeText("${it.key()} DETECTED NEW GT VALUE ${it.value()}", true)
                                            if (it.value() == null) {
                                                skipUpdate.add(it.key())
                                            }
                                        } else {
                                            if (skipUpdate.contains(it.key())) {
                                                skipUpdate.remove(it.key())
                                            }
                                            if (gtCache[it.key()] == null) {
                                                // Investigate.writeText("${it.key()} DETECTED GT VALUE CHANGE FROM NULL TO ${it.value()}", true)
                                            } else {
                                                // Investigate.writeText("${it.key()} DETECTED GT VALUE CHANGE FROM ${(GtBaseFromProto(keyAsByteArray(it.key()), gtCache[it.key()]) as GtValue).toGtProto().second} TO ${it.value()}", true)
                                            }
                                        }
                                        gtCache[it.key()] = gtAsBytes
                                        resultListChangesToGTCache.add(Pair(gtValue.aktoerId, gtAsBytes))
                                    }
                                }
                                else -> {
                                    log.error { "Work Gt parse error" }
                                    exitReason = ExitReason.KafkaIssues
                                    return@consume KafkaConsumerStates.HasIssues
                                }
                            }
                        }
                        else -> {
                            exitReason = ExitReason.KafkaIssues
                            return@consume KafkaConsumerStates.HasIssues
                        }
                    }
                }
            }
        }
        if (exitReason.isOK()) {
            KafkaConsumerStates.IsOk
        } else {
            KafkaConsumerStates.HasIssues
        }
    }

    if (!exitReason.isOK()) {
        log.error { "Aborting gt update due to kafka issue" }
        return exitReason
    }

    log.info { "Work Gt Read of gt done. resultListChangesToGTCache size ${resultListChangesToGTCache.size} processed offset from gt queue $first to $lastOffset " }

    if (resultListChangesToGTCache.size > 0) {
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerGcp
        ).produce {
            resultListChangesToGTCache.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    this.send(kafkaProducerTopicGt, keyAsByteArray(pair.first), it).also {
                        // Investigate.writeText("${pair.first} UPDATE GT VALUE IN $kafkaProducerTopicGt", true)
                        workMetrics.gtPublished.inc()
                    }
                } ?: this.sendNullValue(kafkaProducerTopicGt, keyAsByteArray(pair.first)).also {
                    // Investigate.writeText("${pair.first} UPDATE GT TO NULL IN $kafkaProducerTopicGt", true)
                    workMetrics.gtPublishedTombstone.inc()
                }
            }.let { sent ->
                if (!sent) {
                    workMetrics.producerIssues.inc()
                    log.error { "Gt load - Gt Cache Producer had issues sending to topic" }
                    exitReason = ExitReason.KafkaIssues
                }
            }
        }

        log.info { "Work Gt Post new gt done. resultListChangesToGTCache size ${resultListChangesToGTCache.size} processed offset from gt queue $first to $lastOffset " }
    }

    // Investigate.writeText("SKIPUPDATELIST: $skipUpdate", true)

    // log.info { "Work Commence produce resultListChangesToGTGache of size ${resultListChangesToGTCache.size}" }

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerGcp
    ).produce {
        resultListChangesToGTCache.asSequence().filter { !skipUpdate.contains(it.first) }.map { Pair(it.first, personCache[it.first]) }.filter {
            if (it.second == null) {
                // Investigate.writeText("${it.first} Skipped update from GT due to tombstone in Person cache", true)
            }
            it.second != null
        }.map {
            if (gtCache[it.first] == null) {
                // Updated GT part to null
                (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
            } else {
                val gtBase = GtBaseFromProto(keyAsByteArray(it.first), gtCache[it.first])
                if (gtBase is GtValue) {
                    (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = gtBase.kommunenummerFraGt, bydelsnummerFraGt = gtBase.bydelsnummerFraGt)
                } else {
                    exitReason = ExitReason.KafkaIssues
                    workMetrics.consumerIssues.inc()
                    log.error { "Fail to parse gt from gt cache at update person" }
                    // Investigate.writeText("${it.first} ERROR Fail to parse gt from gt cache at update person", true)
                    (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                }
            }
        }
                .map {
                    personCache[it.aktoerId] = it.toPersonProto().second.toByteArray()
                    // Investigate.writeText("${it.aktoerId} PERSON UPDATE DUE TO GT UPDATE, Value: $it", true)
                    it.toPersonProto()
                }
                .fold(true) { acc, pair ->
                    acc && pair.second.let {
                        send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also {
                            workMetrics.publishedPersons.inc()
                            workMetrics.published_by_gt_update.inc()
                        }
                    }
                }.let { sent ->
                    when (sent) {
                        true -> KafkaConsumerStates.IsOk
                        false -> KafkaConsumerStates.HasIssues.also {
                            exitReason = ExitReason.KafkaIssues
                            workMetrics.producerIssues.inc()
                            log.error { "Producer person for gt update has issues sending to topic" }
                            // Investigate.writeText("Producer person for gt update has issues sending to topic", true)
                        }
                    }
                }
    }

    log.info { "Work Gt Posted affected persons done. resultListChangesToGTCache size ${resultListChangesToGTCache.size} processed offset from gt queue $first to $lastOffset " }

    firstOffsetLimitGt = lastOffset
    // if (!exitReason.isOK()) return@consume KafkaConsumerStates.HasIssues

    // KafkaConsumerStates.IsOk // Continue consume cycle

    workMetrics.gt_cache_size_total.set(gtCache.size.toDouble())
    workMetrics.gt_cache_size_tombstones.set(gtCache.values.filter { it == null }.count().toDouble())

    log.info {
        "Work GT, exit reason is ok? ${exitReason.isOK()} - new messages <persons/tombstones> :  (${workMetrics.gt_cache_new.get().toInt() + workMetrics.gt_cache_update.get().toInt() + workMetrics.gt_cache_blocked.get().toInt()}/${workMetrics.gt_cache_blocked_tombstone.get().toInt() + workMetrics.gt_cache_update_tombstone.get().toInt() + workMetrics.gt_cache_new_tombstone.get().toInt()})" +
                ". Cache Gt new ${workMetrics.gt_cache_new.get().toInt()} update ${workMetrics.gt_cache_update.get().toInt()} blocked ${workMetrics.gt_cache_blocked.get().toInt()}" +
                ", Tombstones new ${workMetrics.gt_cache_new_tombstone.get().toInt()} update ${workMetrics.gt_cache_update_tombstone.get().toInt()} blocked ${workMetrics.gt_cache_blocked_tombstone.get().toInt()}\n" +
                "After gt (and updating ${workMetrics.published_by_gt_update.get().toInt()} person by gt change) publish. Current gt cache size ${workMetrics.gt_cache_size_total.get().toInt()} of which are tombstones ${workMetrics.gt_cache_size_tombstones.get().toInt()}"
    }
    return exitReason
}

var samplesLeft = 5

var presampleLeft = 3

var lifetime = 0

var limitPersonOffset = -1L

fun trysamplequeue() {
    log.info { "SAMPLEQUEUE START" }
    var retries = 5
    val topic = "aapen-person-pdl-dokument-v1-tagged"
    AKafkaConsumer<String, String?>(
        config = ws.kafkaConsumerOnPrem,
        fromBeginning = false,
        topics = listOf(topic)
    ).consume { cRecords ->
        // exitReason = ExitReason.NoEvents
        if (cRecords.isEmpty) {
            if (workMetrics.recordsParsed.get().toInt() == 0 && retries > 0) {
                // exitReason = ExitReason.NoEvents
                log.info { "SAMPLEQUEUE: No records found $retries retries left, wait 60 w" }
                retries--
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            } else {
                log.info { "SAMPLEQUEUE No more records found (or given up) - end consume session" }
                return@consume KafkaConsumerStates.IsFinished
            }
        }
        log.info { "SAMPLEQUEUE - Found ${cRecords.count()} on a batch from topic $topic" }
        File("/tmp/samplequeue").writeText(cRecords.last().value() ?: "null")
        KafkaConsumerStates.IsFinished
    }
    log.info { "SAMPLEQUEUE END" }
}

internal fun work(): ExitReason {
    // var sampleTakenThisWorkSession = false
    log.info { "bootstrap work session starting, lifetime ${++lifetime}" }
    // return ExitReason.NoEvents
    workMetrics.clearAll()

    if (personCache.isEmpty() || gtCache.isEmpty()) {
        log.warn { "Aborting work session since a cache is lacking content. Have person and gt cache been initialized?" }
        return ExitReason.NoCache
    }

    log.info { "Commence updateGtCacheAndAffectedPersons" }
    var exitReason = updateGtCacheAndAffectedPersons()
    log.info { "done updateGtCacheAndAffectedPersons" }

    if (!exitReason.isOK()) {
        log.warn { "Aborting work session since update of gt cache and affected persons returned NOK" }
        return exitReason
    }

    log.info { "Work session - gt update done successfully, person update starting" }
    var retries = 5

    exitReason = ExitReason.NoKafkaProducer

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerGcp
    ).produce {
        exitReason = ExitReason.NoKafkaConsumer
        currentConsumerMessageHost = "PERSON_ONPREM"
        AKafkaConsumer<String, String?>(
                config = ws.kafkaConsumerOnPrem,
                fromBeginning = false
        ).consume { cRecords ->
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) {
                if (workMetrics.recordsParsed.get().toInt() == 0 && retries > 0) {
                    exitReason = ExitReason.NoEvents
                    log.info { "Work: No records found $retries retries left, wait 60 w" }
                    retries--
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    log.info { "Work: No more records found (or given up) - end consume session" }
                    return@consume KafkaConsumerStates.IsFinished
                }
            }
            exitReason = ExitReason.Work
            workMetrics.recordsParsed.inc(cRecords.count().toDouble())

            if (cRecords.last().offset() < limitPersonOffset) {
                log.error { "Kafka person consumed with last offset ${cRecords.last().offset()} is before limit $limitPersonOffset. Abort" }
                exitReason = ExitReason.KafkaIssues
                return@consume KafkaConsumerStates.HasIssues
            }

            val results = cRecords.map { cr ->
                if (cr.value() == null) {
                    val personTombestone = PersonTombestone(aktoerId = cr.key())
                    workMetrics.tombstones.inc()
                    // Investigate.writeText("CONSUMED PERSON OFFSET ${cr.offset()} TOMBSTONE", true)
                    Triple(KafkaConsumerStates.IsOk, personTombestone, cr.offset())
                } else {
                    when (val query = cr.value()!!.getQueryFromJson()) {
                        InvalidQuery -> {
                            workMetrics.consumerIssues.inc()
                            log.error { "Unable to parse topic value PDL" }
                            exitReason = ExitReason.KafkaIssues
                            // TODO SIT: Let trash data pass by
                            Triple(KafkaConsumerStates.IsOk, PersonInvalid, cr.offset())
                        }
                        is Query -> {
                            when (val personSf = query.toPersonSf()) {
                                is PersonSf -> {
                                    /*
                                    if (presampleLeft > 0) {
                                        File("/tmp/presample$presampleLeft").appendText("Sample query json:\n${cr.value()}\n\nSample query:\n" +
                                                "${query}\n\nSample json:\n${personSf.toJson()}}")
                                        presampleLeft--
                                    }
                                     */
                                    // Investigate.writeText("CONSUMED PERSON OFFSET ${cr.offset()} PERSON ID ${personSf.aktoerId}", true)
                                    /*
                                    if (!sampleTakenThisWorkSession) {
                                        log.info("Taking sample for this work session")
                                        sampleTakenThisWorkSession = true
                                        Investigate.writeText("Sample:\n${cr.value()}")
                                    }

                                     */
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
                                                exitReason = ExitReason.KafkaIssues
                                                log.error { "Fail to parse gt from gt cache" }
                                                personSf
                                            }
                                        }
                                        workMetrics.measurePersonStats(enriched)
                                        // Investigate.writeText("ENRICHED FROM VALUE $personSf TO $enriched", true)
                                        Triple(KafkaConsumerStates.IsOk, enriched, cr.offset())
                                    } else {
                                        workMetrics.measurePersonStats(personSf)
                                        Triple(KafkaConsumerStates.IsOk, personSf, cr.offset())
                                    }
                                }
                                is PersonInvalid -> {
                                    workMetrics.consumerIssues.inc()
                                    exitReason = ExitReason.KafkaIssues
                                    Triple(KafkaConsumerStates.HasIssues, PersonInvalid, cr.offset())
                                }
                                else -> {
                                    workMetrics.consumerIssues.inc()
                                    exitReason = ExitReason.KafkaIssues
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
                        is PersonInvalid -> {
                            // Sit specific : Filter away invalid data
                            false
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
                            // Investigate.writeText("${(it.second as PersonTombestone).aktoerId} UPDATE PERSON TO TOMBSTONE", true)
                            Triple<PersonProto.PersonKey, PersonProto.PersonValue?, Long>(personBase.toPersonTombstoneProtoKey(), null, it.third)
                        }
                        is PersonSf -> {
                            // Investigate.writeText("${(it.second as PersonSf).aktoerId} UPDATE PERSON TO VALUE: ${(it.second as PersonSf)}", true)
                            /*
                            if (samplesLeft > 0 && (personBase as PersonSf).identer.isNotEmpty() || (personBase as PersonSf).folkeregisteridentifikator.isNotEmpty()) {


                                log.info { "Sampled published" }
                                File("/tmp/investigate").appendText("Sample json:\n${(personBase as PersonSf).toJson()}\n}")
                                samplesLeft--
                            }*/
                            val protoPair = personBase.toPersonProto()
                            Triple(protoPair.first, protoPair.second, it.third)
                        }
                        else -> return@consume KafkaConsumerStates.HasIssues
                    }
                }.fold(true) { acc, triple ->
                    acc && triple.second?.let {
                        send(kafkaPersonTopic, triple.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc(); workMetrics.latestPublishedPersonsOffset.set(triple.third.toDouble()); limitPersonOffset = cRecords.last().offset() }
                    } ?: sendNullValue(kafkaPersonTopic, triple.first.toByteArray()).also {
                        workMetrics.publishedTombstones.inc()
                    }
                }.let { sent ->
                    when (sent) {
                        true -> KafkaConsumerStates.IsOk
                        false -> KafkaConsumerStates.HasIssues.also {
                            workMetrics.producerIssues.inc()
                            exitReason = ExitReason.KafkaIssues
                            log.error { "Producer new/updated person has issues sending to topic" }
                            // Investigate.writeText("Producer new/updated person has issues sending to topic", true)
                        }
                    }
                }
            } else {
                log.error { "Issue found" }
                exitReason = ExitReason.KafkaIssues
                KafkaConsumerStates.HasIssues
            }
        } // Consumer pdl topic
    } // Producer person topic

    log.info {
        "Work persons - Published new persons: ${workMetrics.publishedPersons.get().toInt()} (lastest offset ${workMetrics.latestPublishedPersonsOffset}), new tombstones: ${workMetrics.publishedTombstones.get().toInt()}" +
                ". Cache enrich actions: ${workMetrics.enriching_from_gt_cache.get().toInt()} person new: ${workMetrics.cache_new.get().toInt()} update: ${workMetrics.cache_update.get().toInt()} blocked: ${workMetrics.cache_blocked.get().toInt()}" +
                ", Tombstones new: ${workMetrics.cache_new_tombstone.get().toInt()} update: ${workMetrics.cache_update_tombstone.get().toInt()} blocked: ${workMetrics.cache_blocked_tombstone.get().toInt()}"
    }

    workMetrics.cache_size_total.set(personCache.size.toDouble())
    workMetrics.cache_size_tombstones.set(personCache.values.filter { it == null }.count().toDouble())

    log.info { "Work - Current final person cache size ${workMetrics.cache_size_total.get().toInt()} of which are tombstones ${workMetrics.cache_size_tombstones.get().toInt()}" }

    workMetrics.logWorkSessionStats()
    log.info { "bootstrap work session finished" }

    if (!exitReason.isOK()) {
        log.error { "Work session has exited with NOK" }
    }
    workMetrics.busy.set(0.0)

    return exitReason
}

internal fun sleepInvestigate() {
    workMetrics.clearAll()
    log.info { "SLEEP GT Cache - load" }
    currentConsumerMessageHost = "SLEEP_GT_CACHE"
    AKafkaConsumer<ByteArray, ByteArray?>(
            config = ws.kafkaConsumerGcp,
            fromBeginning = true,
            topics = listOf(kafkaProducerTopicGt)
    ).consume { consumerRecords ->
        if (consumerRecords.isEmpty) {
            if (workMetrics.gtCacheRecordsParsed.get().toInt() == 0) {
                log.info { "SLEEP GT Cache - retry connection after waiting 60 s" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        workMetrics.gtCacheRecordsParsed.inc(consumerRecords.count().toDouble())
        return@consume KafkaConsumerStates.IsFinished
    }

    log.info { "SLEEP Person Cache - load" }
    currentConsumerMessageHost = "SLEEP_PERSON_CACHE"
    AKafkaConsumer<ByteArray, ByteArray?>(
            config = ws.kafkaConsumerGcp,
            fromBeginning = true,
            topics = listOf(kafkaProducerTopicGt)
    ).consume { consumerRecords ->
        if (consumerRecords.isEmpty) {
            if (workMetrics.cacheRecordsParsed.get().toInt() == 0) {
                log.info { "SLEEP Person Cache - retry connection after waiting 60 s" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        workMetrics.cacheRecordsParsed.inc(consumerRecords.count().toDouble())
        return@consume KafkaConsumerStates.IsFinished
    }

    log.info { "SLEEP Cache load test. Connects with Gt: ${(workMetrics.gtCacheRecordsParsed.get().toInt() != 0)} Person: ${(workMetrics.cacheRecordsParsed.get().toInt() != 0)}" }
}

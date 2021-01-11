package no.nav.sf.pdl

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
    object NoCache : ExitReason()object InvalidCache : ExitReason()
    object Work : ExitReason()

    fun isOK(): Boolean = this is Work || this is NoEvents
}

internal fun updateGtCacheAndAffectedPersons(): ExitReason {
    var gtRetries = 5

    var exitReason: ExitReason = ExitReason.NoKafkaConsumer

    currentConsumerMessageHost = "GT_ONPREM"
    AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerOnPrem,
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
        val resultList: MutableList<Pair<String, ByteArray?>> = mutableListOf()
        workMetrics.gtRecordsParsed.inc(consumerRecords.count().toDouble())
        consumerRecords.forEach {
            if (it.value() == null) {
                if (gtCache.containsKey(it.key()) && gtCache[it.key()] == null) {
                    workMetrics.gt_cache_blocked_tombstone.inc()
                } else {
                    if (gtCache.containsKey(it.key())) workMetrics.gt_cache_update_tombstone.inc() else workMetrics.gt_cache_new_tombstone.inc()
                    gtCache[it.key()] = null
                    resultList.add(Pair(it.key(), null))
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
                                        gtCache[it.key()] = gtAsBytes
                                        resultList.add(Pair(gtValue.aktoerId, gtAsBytes))
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

        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerGcp
        ).produce {
            resultList.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    this.send(kafkaProducerTopicGt, keyAsByteArray(pair.first), it).also { workMetrics.gtPublished.inc() }
                } ?: this.sendNullValue(kafkaProducerTopicGt, keyAsByteArray(pair.first)).also { workMetrics.gtPublishedTombstone.inc() }
            }.let { sent ->
                if (!sent) {
                    workMetrics.producerIssues.inc()
                    log.error { "Gt load - Gt Cache Producer had issues sending to topic" }
                    exitReason = ExitReason.KafkaIssues
                }
            }
        }

        if (!exitReason.isOK()) return@consume KafkaConsumerStates.HasIssues

        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerGcp
        ).produce {
            resultList.map { Pair(it.first, personCache[it.first]) }.filter { it.second != null }.map {
                if (gtCache[it.first] == null) {
                    (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                } else {
                    val gtBase = GtBaseFromProto(keyAsByteArray(it.first), gtCache[it.first])
                    if (gtBase is GtValue) {
                        (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = gtBase.kommunenummerFraGt, bydelsnummerFraGt = gtBase.bydelsnummerFraGt)
                    } else {
                        exitReason = ExitReason.KafkaIssues
                        workMetrics.consumerIssues.inc()
                        log.error { "Fail to parse gt from gt cache at update person" }
                        (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                    }
                }
            }
                    .map {
                        personCache[it.aktoerId] = it.toPersonProto().second.toByteArray()
                        it.toPersonProto()
                    }
                    .fold(true) { acc, pair ->
                        acc && pair.second.let {
                            send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc(); workMetrics.published_by_gt_update.inc() }
                        }
                    }.let { sent ->
                        when (sent) {
                            true -> KafkaConsumerStates.IsOk
                            false -> KafkaConsumerStates.HasIssues.also {
                                exitReason = ExitReason.KafkaIssues
                                workMetrics.producerIssues.inc()
                                log.error { "Producer person for gt update has issues sending to topic" }
                            }
                        }
                    }
        }

        if (!exitReason.isOK()) return@consume KafkaConsumerStates.HasIssues

        KafkaConsumerStates.IsOk // Continue consume cycle
    }

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

internal fun work(): ExitReason {
    log.info { "bootstrap work session starting" }
    workMetrics.clearAll()

    if (personCache.isEmpty() || gtCache.isEmpty()) {
        log.warn { "Aborting work session since a cache is lacking content. Have person and gt cache been initialized?" }
        return ExitReason.NoCache
    }

    var exitReason = updateGtCacheAndAffectedPersons()

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
        AKafkaConsumer<String, String>(
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

            val results = cRecords.map { cr ->
                if (cr.value() == null) {
                    val personTombestone = PersonTombestone(aktoerId = cr.key())
                    workMetrics.tombstones.inc()
                    Pair(KafkaConsumerStates.IsOk, personTombestone)
                } else {
                    when (val query = cr.value().getQueryFromJson()) {
                        InvalidQuery -> {
                            workMetrics.consumerIssues.inc()
                            log.error { "Unable to parse topic value PDL" }
                            exitReason = ExitReason.KafkaIssues
                            Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
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
                                                exitReason = ExitReason.KafkaIssues
                                                log.error { "Fail to parse gt from gt cache" }
                                                personSf
                                            }
                                        }
                                        workMetrics.measurePersonStats(enriched)
                                        Pair(KafkaConsumerStates.IsOk, enriched)
                                    } else {
                                        workMetrics.measurePersonStats(personSf)
                                        Pair(KafkaConsumerStates.IsOk, personSf)
                                    }
                                }
                                is PersonInvalid -> {
                                    workMetrics.consumerIssues.inc()
                                    exitReason = ExitReason.KafkaIssues
                                    Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
                                }
                                else -> {
                                    workMetrics.consumerIssues.inc()
                                    exitReason = ExitReason.KafkaIssues
                                    log.error { "Returned unhandled PersonBase from Query.toPersonSf" }
                                    Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
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
                }.map {
                    when (val personBase = it.second) {
                        is PersonTombestone -> {
                            Pair<PersonProto.PersonKey, PersonProto.PersonValue?>(personBase.toPersonTombstoneProtoKey(), null)
                        }
                        is PersonSf -> {
                            personBase.toPersonProto()
                        }
                        else -> return@consume KafkaConsumerStates.HasIssues
                    }
                }.fold(true) { acc, pair ->
                    acc && pair.second?.let {
                        send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc() }
                    } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also {
                        workMetrics.publishedTombstones.inc()
                    }
                }.let { sent ->
                    when (sent) {
                        true -> KafkaConsumerStates.IsOk
                        false -> KafkaConsumerStates.HasIssues.also {
                            workMetrics.producerIssues.inc()
                            exitReason = ExitReason.KafkaIssues
                            log.error { "Producer new/updated person has issues sending to topic" }
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
        "Work persons - Published new persons: ${workMetrics.publishedPersons.get().toInt()}, new tombstones: ${workMetrics.publishedTombstones.get().toInt()}" +
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

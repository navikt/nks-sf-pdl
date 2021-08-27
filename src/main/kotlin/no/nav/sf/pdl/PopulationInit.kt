package no.nav.sf.pdl

import kotlin.streams.toList
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerRecord

private val log = KotlinLogging.logger {}

var initReference = 0

fun keyAsByteArray(key: String): ByteArray {
    return PersonProto.PersonKey.newBuilder().apply {
        aktoerId = key
    }.build().toByteArray()
}
internal fun parsePdlJsonOnInit(cr: ConsumerRecord<String, String?>): PersonBase {
    if (cr.value() == null) {
        workMetrics.initialTombstones.inc()
        return PersonTombestone(aktoerId = cr.key())
    } else {
        when (val query = cr.value()?.getQueryFromJson() ?: InvalidQuery) {
            InvalidQuery -> {
                log.error { "Init: Unable to parse topic value as query object" }
                return PersonInvalid
            }
            is Query -> {
                when (val personSf = query.toPersonSf()) {
                    is PersonSf -> {
                        workMetrics.initialPersons.inc()
                        return personSf
                    }
                    is PersonInvalid -> {
                        log.error { "Init: Failed to parse person" }
                        return PersonInvalid
                    }
                    else -> {
                        log.error { "Init: Unhandled PersonBase from Query.toPersonSf" }
                        return PersonInvalid
                    }
                }
            }
        }
    }
}

fun List<Pair<String, PersonBase>>.isValid(): Boolean {
    return this.map { it.second }.filterIsInstance<PersonInvalid>().isEmpty() && this.map { it.second }.filterIsInstance<PersonProtobufIssue>().isEmpty()
}

var heartBeatConsumer: Int = 0

var retry: Int = 0

internal fun initLoadTest() {
    var interestingHitCount = 0
    var count = 0
    log.info { "Start init test" }
    workMetrics.testRunRecordsParsed.clear()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerOnPremSeparateClientId,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    // val resultListTest: MutableList<String> = mutableListOf()

    while (workMetrics.testRunRecordsParsed.get() == 0.0) {
        kafkaConsumerPdlTest.consume { cRecords ->
            if (cRecords.isEmpty) {
                if (workMetrics.testRunRecordsParsed.get() == 0.0) {
                    log.info { "Init test run: Did not get any messages on retry ${++retry}, will wait 60 s and try again" }
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    return@consume KafkaConsumerStates.IsFinished
                }
            }

            count += cRecords.count()
            workMetrics.testRunRecordsParsed.inc(cRecords.count().toDouble())
            cRecords.filter { it.key() == "1000013140246" || it.value()?.contains("1000013140246") ?: false }.forEach {
                log.info { "INVESTIGATE - found interesting one" }
                interestingHitCount++
                Investigate.writeText(it.value() ?: "null", true, "/tmp/search")
            }

            if (heartBeatConsumer == 0) {
                log.info { "Init test run: Successfully consumed a batch (This is prompted at start and each 100000th consume batch)" }
            }

            heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)
            KafkaConsumerStates.IsOk
        }
        heartBeatConsumer = 0
    }
    log.info { "INVESTIGATE - done Init test run, Interesting hit count: $interestingHitCount, Count $count, Total records from topic: ${workMetrics.testRunRecordsParsed.get().toInt()}" }
    // workMetrics.testRunRecordsParsed.set(resultListTest.size.toDouble())
    // initReference = resultListTest.stream().distinct().toList().size
    // log.info { "Init test run : Total unique records from topic: $initReference" }
}

internal fun initLoad(): ExitReason {
    workMetrics.clearAll()
    retry = 0

    log.info { "Init: Commencing reading all records on topic $kafkaPDLTopic" }

    val resultList: MutableList<Pair<String, ByteArray?>> = mutableListOf()

    log.info { "Defining Consumer for pdl read" }
    val kafkaConsumerPdl = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerOnPrem,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    var initParseBatchOk = true

    while (workMetrics.initialRecordsParsed.get() == 0.0) {
        kafkaConsumerPdl.consume { cRecords ->
            if (heartBeatConsumer == 0) {
                log.info { "Init: Fetched a set of records (This is prompted first and each 10000th consume batch) Records size: ${cRecords.count()}" }
            }
            if (cRecords.isEmpty) {
                if (workMetrics.initialRecordsParsed.get() == 0.0) {
                    log.info { "Init: Did not get any messages on retry ${++retry}, will wait 60 s and try again" }
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    log.info { "Init: I find no more records from topic. Will finish" }
                    return@consume KafkaConsumerStates.IsFinished
                }
            }
            val parsedBatch: List<Pair<String, PersonBase>> = cRecords.map { cr ->
                Pair(cr.key(), parsePdlJsonOnInit(cr))
            }
            if (heartBeatConsumer == 0) {
                log.info { "Init: Successfully consumed a batch (This is prompted first and each 10000th consume batch) Batch size: ${parsedBatch.size}" }
            }
            heartBeatConsumer = ((heartBeatConsumer + 1) % 10000)

            if (parsedBatch.isValid()) {
                // TODO Any statistics checks on person values from topic (before distinct/latest per key) can be added here:
                workMetrics.initialRecordsParsed.inc(cRecords.count().toDouble())
                parsedBatch.forEach {
                    when (val personBase = it.second) {
                        is PersonTombestone -> {
                            resultList.add(Pair(it.first, null))
                        }
                        is PersonSf -> {
                            val personEnriched =
                                    if (gtCache.containsKey(personBase.aktoerId)) {
                                        workMetrics.enriching_from_gt_cache.inc()
                                        if (gtCache[personBase.aktoerId] == null) {
                                            personBase.copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                                        } else {
                                            val gtBase = GtBaseFromProto(keyAsByteArray(personBase.aktoerId), gtCache[personBase.aktoerId])
                                            if (gtBase is GtValue) {
                                                personBase.copy(kommunenummerFraGt = gtBase.kommunenummerFraGt, bydelsnummerFraGt = gtBase.bydelsnummerFraGt)
                                            } else {
                                                workMetrics.consumerIssues.inc()
                                                log.error { "Fail to parse gt from gt cache" }
                                                personBase
                                            }
                                        }
                                    } else {
                                        personBase
                                    }
                            val personProto = personEnriched.toPersonProto()
                            resultList.add(Pair(it.first, personProto.second.toByteArray()))
                        }
                        else -> {
                            log.error { "Should never arrive here" }; initParseBatchOk = false; KafkaConsumerStates.HasIssues
                        }
                    }
                }
                KafkaConsumerStates.IsOk
            } else {
                initParseBatchOk = false
                workMetrics.consumerIssues.inc()
                KafkaConsumerStates.HasIssues
            }
        }
    }
    if (!initParseBatchOk) {
        log.error { "Init: Result contains invalid records" }
        return ExitReason.InvalidCache
    }
    log.info { "Init: Number of records from topic $kafkaPDLTopic is ${resultList.size}" }
    val latestRecords = resultList.toMap() // Remove duplicates. Done after topic consuming is finished due to updating a map of ~10M entries crashed/stalled consuming phase
    log.info { "Init: Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }
    if (initReference > 0) {
        if (latestRecords.size >= initReference) {
            log.info { "Init: reference has been run: map deemed healthy since it contains more or equals ids then reference" }
        } else {
            log.error { "Init: reference has been run: map deemed unhealthy since it contains less then reference" }
        }
    }
    // Measuring round
    latestRecords.forEach {
        when (val personBase = PersonBaseFromProto(keyAsByteArray(it.key), it.value)) {
            // TODO Here we can measure for statistics and make checks for unexpected values:
            is PersonSf -> {
                workMetrics.measurePersonStats(personBase, false)
            }
            is PersonTombestone -> {
                workMetrics.tombstones.inc()
            }
            else -> {
                log.error { "Init: Found unexpected person base when measuring" }
            }
        }
    }

    // File("/tmp/investigate").writeText("Findings:\n${workMetrics.investigateList.map{it.toJson()}.joinToString("\n\n")}")
    // workMetrics.investigateList.clear()

    log.info { "Init: Number of living persons: ${workMetrics.livingPersons.get().toInt()}, dead persons: ${workMetrics.deadPersons.get().toInt()} (of which unknown death date: ${workMetrics.deadPersonsWithoutDate.get().toInt()}), tombstones: ${workMetrics.tombstones.get().toInt()}" }
    log.info { "Init: Earliest death date: ${workMetrics.earliestDeath.toIsoString()}" }
    var exitReason: ExitReason = ExitReason.Work // ExitReason.NoKafkaProducer
    var producerCount = 0
    // TODO Uncomment for init load publishing

    log.info { "Init: Undertake producing to size ${latestRecords.size}" }

    latestRecords.toList().asSequence().chunked(500000).forEach {
        log.info { "Init: Creating aiven producer for batch ${producerCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerGcp
        ).produce {
            it.fold(true) { acc, pair ->
                acc && pair.second?.let { this.send(kafkaPersonTopic, keyAsByteArray(pair.first), it).also { workMetrics.initialPublishedPersons.inc() }
                } ?: this.sendNullValue(kafkaPersonTopic, keyAsByteArray(pair.first)).also { workMetrics.initialPublishedTombstones.inc() }
            }.let { sent ->
                when (sent) {
                    true -> exitReason = ExitReason.Work
                    false -> {
                        exitReason = ExitReason.InvalidCache
                        workMetrics.producerIssues.inc()
                        log.error { "Init load - Producer $producerCount has issues sending to topic" }
                    }
                }
            }
        }
    }

    workMetrics.logInitialLoadStats()

    log.info { "Init load - Ok? ${exitReason.isOK()} " }

    return exitReason
}

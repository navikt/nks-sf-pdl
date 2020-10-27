package no.nav.sf.pdl

import kotlin.streams.toList
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerRecord

private val log = KotlinLogging.logger {}

internal fun parsePdlJsonOnInit(cr: ConsumerRecord<String, String?>): PersonBase {
    if (cr.value() == null) {
        workMetrics.noOfInitialTombestone.inc()
        return PersonTombestone(aktoerId = cr.key())
    } else {
        when (val query = cr.value()?.getQueryFromJson() ?: InvalidQuery) {
            InvalidQuery -> {
                log.error { "Init: Unable to parse topic value PDL Message:\n${cr.value()}" }
                workMetrics.invalidPersonsParsed.inc()
                return PersonInvalid
            }
            is Query -> {
                when (val personSf = query.toPersonSf()) {
                    is PersonSf -> {
                        workMetrics.noOfInitialPersonSf.inc()
                        return personSf
                    }
                    is PersonInvalid -> {
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                    else -> {
                        log.error { "Init: Unhandled PersonBase from Query.toPersonSf" }
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                }
            }
        }
    }
}

fun List<Pair<String, PersonBase>>.isValid(): Boolean {
    return this.filterIsInstance<PersonInvalid>().isEmpty() && this.filterIsInstance<PersonProtobufIssue>().isEmpty()
}

var heartBeatConsumer: Int = 0

var retry: Int = 0

internal fun initLoadTest(ws: WorkSettings) {
    log.info { "Start init test" }
    workMetrics.initRecordsParsedTest.clear()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdlAlternative,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    val resultListTest: MutableList<String> = mutableListOf()

    while (workMetrics.initRecordsParsedTest.get() == 0.0) {
        kafkaConsumerPdlTest.consume { cRecords ->
            if (cRecords.isEmpty) {
                if (workMetrics.initRecordsParsedTest.get() == 0.0) {
                    log.info { "Did not get any messages on retry ${++retry}, will wait 60 s and try again" }
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    return@consume KafkaConsumerStates.IsFinished
                }
            }

            workMetrics.initRecordsParsedTest.inc(cRecords.count().toDouble())
            cRecords.forEach { cr -> resultListTest.add(cr.key()) }

            if (heartBeatConsumer == 0) {
                log.info { "Init test phase Successfully consumed a batch (This is prompted at start and each 100000th consume batch)" }
            }

            heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)
            KafkaConsumerStates.IsOk
        }
        heartBeatConsumer = 0
    }

    log.info { "Init test run : Total records from topic: ${resultListTest.size}" }
    workMetrics.initRecordsParsedTest.set(resultListTest.size.toDouble())
    log.info { "Init test run : Total unique records from topic: ${resultListTest.stream().distinct().toList().size}" }
}

internal fun initLoad(ws: WorkSettings): ExitReason {
    workMetrics.clearAll()

    log.info { "Init: Commencing reading all records on topic $kafkaPDLTopic" }

    val resultList: MutableList<Pair<ByteArray, ByteArray?>> = mutableListOf()

    val kafkaConsumerPdl = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdlAlternative,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    var initParseBatchOk = true

    while (workMetrics.initRecordsParsed.get() == 0.0) {
        kafkaConsumerPdl.consume { cRecords ->
            if (heartBeatConsumer == 0) {
                log.info { "Init: Fetched a set of crecords (This is prompted first and each 10000th consume batch) Records size: ${cRecords.count()}" }
            }
            if (cRecords.isEmpty) {
                if (workMetrics.initRecordsParsed.get() == 0.0) {
                    log.info { "Did not get any messages on retry ${++retry}, will wait 60 s and try again" }
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    return@consume KafkaConsumerStates.IsFinished
                }
            }
            val parsedBatch: List<Pair<String, PersonBase>> = cRecords.map { cr -> Pair(cr.key(), parsePdlJsonOnInit(cr)) }
            if (heartBeatConsumer == 0) {
                log.info { "Init: Successfully consumed a batch (This is prompted first and each 10000th consume batch) Batch size: ${parsedBatch.size}" }
            }
            heartBeatConsumer = ((heartBeatConsumer + 1) % 10000)

            if (parsedBatch.isValid()) {
                // TODO Any statistics checks on person values from topic (before distinct/latest per key) can be added here:
                workMetrics.initRecordsParsed.inc(cRecords.count().toDouble())
                parsedBatch.map {
                    when (val personBase = it.second) {
                        is PersonTombestone -> {
                            resultList.add(Pair(personBase.toPersonTombstoneProtoKey().toByteArray(), null))
                        }
                        is PersonSf -> {
                            val personSf = it.second as PersonSf

                            if (personSf.kommunenummerFraGt != null && personSf.kommunenummerFraAdresse != null) {
                                workMetrics.kommunenummerFraGt.inc()
                                workMetrics.kommunenummerFraAdresse.inc()
                            } else if (personSf.kommunenummerFraAdresse != null) {
                                workMetrics.kommunenummerFraAdresse.inc()
                            } else if (personSf.kommunenummerFraGt != null) {
                                workMetrics.kommunenummerFraGt.inc()
                            } else if (personSf.kommunenummerFraAdresse == UKJENT_FRA_PDL
                                    && personSf.kommunenummerFraGt == UKJENT_FRA_PDL) {
                                workMetrics.noKommuneNummerFromAdresseOrGt.inc()
                            }

                            val personProto = personBase.toPersonProto()
                            resultList.add(Pair(personProto.first.toByteArray(), personProto.second.toByteArray()))
                        }
                        else -> {
                            log.error { "Should never arrive here" }; initParseBatchOk = false; KafkaConsumerStates.HasIssues
                        }
                    }
                }
                KafkaConsumerStates.IsOk
            } else {
                initParseBatchOk = false
                KafkaConsumerStates.HasIssues
            }
        }
    }
    if (!initParseBatchOk) {
        log.error { "Init: Result contains invalid records" }
        return ExitReason.InvalidCache
    }
    log.info { "Init: Number of records from topic $kafkaPDLTopic is ${resultList.size}" }

    val latestRecords = resultList.toMap() // Remove duplicates. Done after topic consuming is finished (using a map of ~10M crashed consuming phase)

    log.info { "Init: Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }

    var numberOfDeadPeopleFound = 0

    // Measuring round
    latestRecords.forEach {
        when (val personBase = PersonBaseFromProto(it.key, it.value)) {
            // TODO Here we can measure for statistics and make checks for unexpected values:
            is PersonSf -> {
                workMetrics.noOfInitialKakfaRecordsPdl.inc()
                if (personBase.isDead()) numberOfDeadPeopleFound++
            }
            is PersonTombestone -> {
                workMetrics.noOfInitialKakfaRecordsPdl.inc()
            }
            else -> {
                log.error { "Init: Found unexpected person base when measuring" }
            }
        }
    }
    log.info { "Init: Number of aktoersIds that is dead people: $numberOfDeadPeopleFound. Number alive and tombstones: ${workMetrics.noOfInitialKakfaRecordsPdl.get().toInt() - numberOfDeadPeopleFound}" }
    workMetrics.deadPersons.set(numberOfDeadPeopleFound.toDouble())

    var exitReason: ExitReason = ExitReason.NoKafkaProducer
    var producerCount = 0

    latestRecords.toList().asSequence().chunked(500000).forEach {
        log.info { "Init: Creating producer for batch ${producerCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPerson
        ).produce {
            it.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    send(kafkaPersonTopic, pair.first, it).also { workMetrics.initiallyPublishedPersons.inc() }
                } ?: sendNullValue(kafkaPersonTopic, pair.first).also { workMetrics.initiallyPublishedTombestones.inc() }
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

    log.info { "Init load - Done with publishing to topic exitReason is Ok? ${exitReason.isOK()} " }

    return exitReason
}

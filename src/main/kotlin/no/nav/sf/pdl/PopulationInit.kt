package no.nav.sf.pdl

import java.time.LocalDate
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
        workMetrics.initialTombstones.inc()
        return PersonTombestone(aktoerId = cr.key())
    } else {
        when (val query = cr.value()?.getQueryFromJson() ?: InvalidQuery) {
            InvalidQuery -> {
                log.error { "Init: Unable to parse topic value PDL Message:\n${cr.value()}" }
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

internal fun initLoadTest(ws: WorkSettings) {
    log.info { "Start init test" }
    workMetrics.testRunRecordsParsed.clear()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdlAlternative,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    val resultListTest: MutableList<String> = mutableListOf()

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

            workMetrics.testRunRecordsParsed.inc(cRecords.count().toDouble())
            cRecords.forEach { cr -> resultListTest.add(cr.key()) }

            if (heartBeatConsumer == 0) {
                log.info { "Init test run: Successfully consumed a batch (This is prompted at start and each 100000th consume batch)" }
            }

            heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)
            KafkaConsumerStates.IsOk
        }
        heartBeatConsumer = 0
    }

    log.info { "Init test run : Total records from topic: ${resultListTest.size}" }
    workMetrics.testRunRecordsParsed.set(resultListTest.size.toDouble())
    log.info { "Init test run : Total unique records from topic: ${resultListTest.stream().distinct().toList().size}" }
}

internal fun initLoad(ws: WorkSettings): ExitReason {
    retry = 0
    workMetrics.clearAll()

    log.info { "Init: Commencing reading all records on topic $kafkaPDLTopic" }

    val resultList: MutableList<Pair<ByteArray, ByteArray?>> = mutableListOf()

    log.info { "Defining Consumer for pdl read" }
    val kafkaConsumerPdl = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdlAlternative,
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
            val parsedBatch: List<Pair<String, PersonBase>> = cRecords.map { cr -> Pair(cr.key(), parsePdlJsonOnInit(cr)) }
            if (heartBeatConsumer == 0) {
                log.info { "Init: Successfully consumed a batch (This is prompted first and each 10000th consume batch) Batch size: ${parsedBatch.size}" }
            }
            heartBeatConsumer = ((heartBeatConsumer + 1) % 10000)

            if (parsedBatch.isValid()) {
                // TODO Any statistics checks on person values from topic (before distinct/latest per key) can be added here:
                workMetrics.initialRecordsParsed.inc(cRecords.count().toDouble())
                parsedBatch.map {
                    when (val personBase = it.second) {
                        is PersonTombestone -> {
                            resultList.add(Pair(personBase.toPersonTombstoneProtoKey().toByteArray(), null))
                        }
                        is PersonSf -> {
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
    val latestRecords = resultList.toMap() // Remove duplicates. Done after topic consuming is finished (due to updating a map of ~10M entries crashed consuming phase)
    log.info { "Init: Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }

    var earliestDeath: LocalDate? = null
    // Measuring round
    latestRecords.forEach {
        when (val personBase = PersonBaseFromProto(it.key, it.value)) {
            // TODO Here we can measure for statistics and make checks for unexpected values:
            is PersonSf -> {
                if (personBase.isDead()) {
                    workMetrics.deadPersons.inc()
                    if (personBase.doedsfall.any { it.doedsdato != null }) {
                        personBase.doedsfall.filter { it.doedsdato != null }.forEach { doedsfall ->
                            doedsfall.doedsdato?.let {
                                earliestDeath = if (earliestDeath == null) {
                                    it
                                } else {
                                    if (it.isBefore(earliestDeath)) it else earliestDeath
                                }
                            }
                        }
                    } else {
                        workMetrics.deadPersonsWithoutDate.inc()
                    }
                } else {
                    workMetrics.livingPersons.inc()
                }
                when {
                    personBase.kommunenummerFraAdresse == UKJENT_FRA_PDL && personBase.kommunenummerFraGt == UKJENT_FRA_PDL -> {
                        workMetrics.kommunenummerMissing.inc()
                    }
                    personBase.kommunenummerFraGt != UKJENT_FRA_PDL && personBase.kommunenummerFraAdresse == UKJENT_FRA_PDL -> {
                        workMetrics.kommunenummerOnlyFromGt.inc()
                    }
                    personBase.kommunenummerFraGt == UKJENT_FRA_PDL && personBase.kommunenummerFraAdresse != UKJENT_FRA_PDL -> {
                        workMetrics.kommunenummerOnlyFromAdresse.inc()
                    }
                    personBase.kommunenummerFraGt != UKJENT_FRA_PDL && personBase.kommunenummerFraAdresse != UKJENT_FRA_PDL -> {
                        workMetrics.kommunenummerFromBothAdresseAndGt.inc()
                        if (personBase.kommunenummerFraGt == personBase.kommunenummerFraAdresse) {
                            workMetrics.kommunenummerFromAdresseAndGtIsTheSame.inc()
                        } else {
                            workMetrics.kommunenummerFromAdresseAndGtDiffer.inc()
                        }
                    }

                    personBase.bydelsnummerFraAdresse == UKJENT_FRA_PDL && personBase.bydelsnummerFraGt == UKJENT_FRA_PDL -> {
                        workMetrics.bydelsnummerMissing.inc()
                    }
                    personBase.bydelsnummerFraGt != UKJENT_FRA_PDL && personBase.bydelsnummerFraAdresse == UKJENT_FRA_PDL -> {
                        workMetrics.bydelsnummerOnlyFromGt.inc()
                    }
                    personBase.bydelsnummerFraGt == UKJENT_FRA_PDL && personBase.bydelsnummerFraAdresse != UKJENT_FRA_PDL -> {
                        workMetrics.bydelsnummerOnlyFromAdresse.inc()
                    }
                    personBase.bydelsnummerFraGt != UKJENT_FRA_PDL && personBase.bydelsnummerFraAdresse != UKJENT_FRA_PDL -> {
                        workMetrics.bydelsnummerFromBothAdresseAndGt.inc()
                        if (personBase.bydelsnummerFraGt == personBase.bydelsnummerFraAdresse) {
                            workMetrics.bydelsnummerFromAdresseAndGtIsTheSame.inc()
                        } else {
                            workMetrics.bydelsnummerFromAdresseAndGtDiffer.inc()
                        }
                    }
                }
                if (personBase.kommunenummerFraGt != UKJENT_FRA_PDL) {
                    workMetrics.measureKommune(personBase.kommunenummerFraGt)
                } else {
                    workMetrics.measureKommune(personBase.kommunenummerFraAdresse)
                }
            }
            is PersonTombestone -> {
                workMetrics.tombstones.inc()
            }
            else -> {
                log.error { "Init: Found unexpected person base when measuring" }
            }
        }
    }

    log.info { "Init: Number of living persons: ${workMetrics.livingPersons.get().toInt()}, dead persons: ${workMetrics.deadPersons.get().toInt()} (of which unknown death date: ${workMetrics.deadPersonsWithoutDate.get().toInt()}), tombstones: ${workMetrics.tombstones.get().toInt()}" }
    log.info { "Init: Earliest death date: ${earliestDeath.toIsoString()}" }
    var exitReason: ExitReason = ExitReason.NoKafkaProducer
    var producerCount = 0
    log.info { "Init: Undertake producing to size ${latestRecords.size}" }

    latestRecords.toList().asSequence().chunked(500000).forEach {
        log.info { "Init: Creating aiven producer for batch ${producerCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPersonAiven
        ).produce {
            it.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    send(kafkaPersonTopic, pair.first, it).also { workMetrics.initialPublishedPersons.inc() }
                } ?: sendNullValue(kafkaPersonTopic, pair.first).also { workMetrics.initialPublishedTombstones.inc() }
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

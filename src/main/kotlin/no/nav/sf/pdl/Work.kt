package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

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

data class WorkSettings(
    val kafkaProducerPersonAiven: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            "security.protocol" to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword)
    ),
    val kafkaConsumerPersonAiven: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            "security.protocol" to "SSL",
            "ssl.keystore.location" to AnEnvironment.getEnvOrDefault("KAFKA_KEYSTORE_PATH", "KAFKA_KEYSTORE_PATH MISSING"),
            "ssl.keystore.password" to AnEnvironment.getEnvOrDefault("KAFKA_CREDSTORE_PASSWORD", "KAFKA_CREDSTORE_PASSWORD MISSING"),
            "ssl.truststore.location" to AnEnvironment.getEnvOrDefault("KAFKA_TRUSTSTORE_PATH", "KAFKA_TRUSTSTORE_PATH MISSING"),
            "ssl.truststore.password" to AnEnvironment.getEnvOrDefault("KAFKA_CREDSTORE_PASSWORD", "KAFKA_CREDSTORE_PASSWORD MISSING")
    ),
    val kafkaConsumerPdl: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to fetchEnv(EV_kafkaBrokersOnPrem)
    ),
    val kafkaConsumerPdlAlternative: Map<String, Any> = AKafkaConsumer.configAlternativeBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to fetchEnv(EV_kafkaBrokersOnPrem)
    )
)

val workMetrics = WMetrics()

sealed class ExitReason {
    object NoFilter : ExitReason()
    object NoKafkaProducer : ExitReason()
    object NoKafkaConsumer : ExitReason()
    object NoEvents : ExitReason()
    object NoCache : ExitReason()
    object InvalidCache : ExitReason()
    object Work : ExitReason()

    fun isOK(): Boolean = this is Work || this is NoEvents
}

var numberOfWorkSessionsWithoutEvents = 0

var initial_retries_left = 10

internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {
    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()
    var gtTombestones = 0
    var gtSuccess = 0
    var gtFail = 0
    var gtRetries = 5

    if (personCache.isEmpty() || gtCache.isEmpty()) {
        log.warn { "Aborting work session since a cache is lacking content. Have person and gt cache been initialized?" }
        return Pair(ws, ExitReason.NoCache)
    }
    var consumed = 0

    val kafkaConsumer = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdl,
            fromBeginning = false,
            topics = listOf(kafkaGTTopic)
    )

    val resultList: MutableList<Pair<String, ByteArray?>> = mutableListOf()
    kafkaConsumer.consume { consumerRecords ->
        if (consumerRecords.isEmpty) {
            if (workMetrics.gtRecordsParsed.get().toInt() == 0 && gtRetries > 0) {
                gtRetries--
                log.info { "Gt - retry connection after waiting 60 s Retries left: $gtRetries" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        workMetrics.gtRecordsParsed.inc(consumerRecords.count().toDouble())
        consumerRecords.forEach {
            if (it.value() == null) {
                gtTombestones++
                if (gtCache.containsKey(it.key()) && gtCache[it.key()] == null) {
                    workMetrics.gt_cache_blocked_tombstone.inc()
                } else {
                    if (gtCache.containsKey(it.key())) workMetrics.gt_cache_update_tombstone.inc() else workMetrics.gt_cache_new_tombstone.inc()
                    gtCache[it.key()] = null
                    resultList.add(Pair(it.key(), null))
                }
            } else {
                // if (investigate.size < 10) investigate.add(it.value() ?: "")
                it.value()?.let { v ->
                    when (val gt = v.getGtFromJson()) {
                        is Gt -> {
                            when (val gtValue = gt.toGtValue(it.key())) {
                                is GtValue -> {
                                    val gtAsBytes = gtValue.toGtProto().second.toByteArray()
                                    gtSuccess++
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
                                    gtFail++
                                }
                            }
                        }
                        else -> gtFail++
                    }
                }
            }
        }
        KafkaConsumerStates.IsOk
    }

    log.info {
        "Work GT - new messages tombstones $gtTombestones success $gtSuccess fails $gtFail. Will publish ${resultList.size}" +
                ". Cache Gt new ${workMetrics.gt_cache_new.get().toInt()} update ${workMetrics.gt_cache_update.get().toInt()} blocked ${workMetrics.gt_cache_blocked.get().toInt()}" +
                ", Tombstones new ${workMetrics.gt_cache_new_tombstone.get().toInt()} update ${workMetrics.gt_cache_update_tombstone.get().toInt()} blocked ${workMetrics.gt_cache_blocked_tombstone.get().toInt()}"
    }

    workMetrics.gt_cache_size_total.set(gtCache.size.toDouble())
    workMetrics.gt_cache_size_tombstones.set(gtCache.values.filter { it == null }.count().toDouble())
    var producerCount = 0
    resultList.asSequence().chunked(500000).forEach { c ->
        log.info { "Work GT: Creating aiven producer for batch ${producerCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPersonAiven
        ).produce {
            c.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    this.send(kafkaProducerTopicGt, keyAsByteArray(pair.first), it).also { workMetrics.gtPublished.inc() }
                } ?: this.sendNullValue(kafkaProducerTopicGt, keyAsByteArray(pair.first)).also { workMetrics.gtPublishedTombstone.inc() }
                // acc && pair.second?.let {
                //    send(kafkaProducerTopicGt, keyAsByteArray(pair.first), it).also { workMetrics.initialPublishedPersons.inc() }
                // } ?: sendNullValue(kafkaPersonTopic, keyAsByteArray(pair.first)).also { workMetrics.initialPublishedTombstones.inc() }
            }.let { sent ->
                if (!sent) {
                    workMetrics.producerIssues.inc()
                    log.error { "Gt load - Producer $producerCount has issues sending to topic" }
                }
            }
        }
        log.info { "Work GT: Creating aiven person producer for batch ${producerCount++}" }
        val personsInCache: MutableList<PersonSf> = mutableListOf()

        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPersonAiven
        ).produce {
            c.map { Pair(it.first, personCache[it.first]) }.filter { it.second != null }.map {
                if (gtCache[it.first] == null) {
                    (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                } else {
                    val gtBase = GtBaseFromProto(keyAsByteArray(it.first), gtCache[it.first])
                    if (gtBase is GtValue) {
                        (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = gtBase.kommunenummerFraGt, bydelsnummerFraGt = gtBase.bydelsnummerFraGt)
                    } else {
                        workMetrics.consumerIssues.inc()
                        log.error { "Fail to parse gt from gt cache at update" }
                        (PersonBaseFromProto(keyAsByteArray(it.first), it.second) as PersonSf).copy(kommunenummerFraGt = UKJENT_FRA_PDL, bydelsnummerFraGt = UKJENT_FRA_PDL)
                    }
                }
            }
                    .map { it.toPersonProto() }
                    .fold(true) { acc, pair ->
                        acc && pair.second?.let {
                            send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc(); workMetrics.published_by_gt_update.inc() }
                        }
                    }.let { sent ->
                        when (sent) {
                            true -> KafkaConsumerStates.IsOk
                            false -> KafkaConsumerStates.HasIssues.also {
                                workMetrics.producerIssues.inc()
                                log.error { "Producer has issues sending to topic" }
                            }
                        }
                    }
        }
    }

    log.info { "Work GT - After gt (and updating ${workMetrics.published_by_gt_update.get().toInt()} person by gt change) publish. Current gt cache size ${workMetrics.gt_cache_size_total.get().toInt()} of which are tombstones ${workMetrics.gt_cache_size_tombstones.get().toInt()}" }

    var exitReason: ExitReason = ExitReason.NoKafkaProducer

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPersonAiven
    ).produce {

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                fromBeginning = false
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->

            workMetrics.recordsParsed.inc(cRecords.count().toDouble())
            // leaving if nothing to do
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) {
                if (initial_retries_left > 0) {
                    log.info { "Work: No records found $initial_retries_left initial retries left, wait 60 w" }
                    initial_retries_left--
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    log.info { "Work: No records found - end consume session" }
                    return@consume KafkaConsumerStates.IsFinished
                }
            }
            initial_retries_left = 0 // Got data - connection established - no more retries
            log.info { "Work: Consumed a batch of ${cRecords.count()} records" }

            numberOfWorkSessionsWithoutEvents = 0

            exitReason = ExitReason.Work
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
                                    Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
                                }
                                else -> {
                                    workMetrics.consumerIssues.inc()
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
                            }
                            workMetrics.cache_blocked_tombstone.inc()
                            false
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
                            }
                            workMetrics.cache_blocked.inc()
                            false
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
                        true -> KafkaConsumerStates.IsOk.also { consumed += results.size }
                        false -> KafkaConsumerStates.HasIssues.also {
                            workMetrics.producerIssues.inc()
                            log.error { "Producer has issues sending to topic" }
                        }
                    }
                }
            } else {
                KafkaConsumerStates.HasIssues
            }
        } // Consumer pdl topic
    } // Producer person topic

    if (consumed == 0) numberOfWorkSessionsWithoutEvents++

    log.info {
        "Work persons - records consumed $consumed. Published new persons ${workMetrics.publishedPersons.get().toInt()}, new tombstones ${workMetrics.publishedTombstones.get().toInt()}" +
                ". Cache person new ${workMetrics.cache_new.get().toInt()} update ${workMetrics.cache_update.get().toInt()} blocked ${workMetrics.cache_blocked.get().toInt()}" +
                ", Tombstones new ${workMetrics.cache_new_tombstone.get().toInt()} update ${workMetrics.cache_update_tombstone.get().toInt()} blocked ${workMetrics.cache_blocked_tombstone.get().toInt()}"
    }

    workMetrics.cache_size_total.set(personCache.size.toDouble())
    workMetrics.cache_size_tombstones.set(personCache.values.filter { it == null }.count().toDouble())

    log.info { "Work - Current person cache size ${workMetrics.cache_size_total.get().toInt()} of which are tombstones ${workMetrics.cache_size_tombstones.get().toInt()}" }

    log.info { "bootstrap work session finished" }

    if (!exitReason.isOK()) {
        log.error { "Work session has exited with not OK" }
    }

    return Pair(ws, exitReason)
}

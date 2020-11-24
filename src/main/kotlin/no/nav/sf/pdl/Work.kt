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
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
const val EV_kafkaSchemaReg = "KAFKA_SCREG"
const val EV_kafkaBrokersOnPrem = "KAFKA_BROKERS_ON_PREM"

// Environment dependencies injected in pod by kafka solution
const val EV_kafkaKeystorePath = "KAFKA_KEYSTORE_PATH"
const val EV_kafkaCredstorePassword = "KAFKA_CREDSTORE_PASSWORD"
const val EV_kafkaTruststorePath = "KAFKA_TRUSTSTORE_PATH"

val kafkaSchemaReg = AnEnvironment.getEnvOrDefault(EV_kafkaSchemaReg, "http://localhost:8081")
val kafkaPersonTopic = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopic, "$PROGNAME-producer")
val kafkaPDLTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")

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

    var consumed = 0

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
                                    workMetrics.measurePersonStats(personSf)
                                    Pair(KafkaConsumerStates.IsOk, personSf)
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
                results.map {
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

    log.info { "bootstrap work session finished, records consumed $consumed" }

    if (!exitReason.isOK()) {
        log.error { "Work session has exited with not OK" }
    }

    return Pair(ws, exitReason)
}

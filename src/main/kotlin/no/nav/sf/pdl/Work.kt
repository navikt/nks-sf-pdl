package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AVault
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
const val EV_kafkaSchemaReg = "KAFKA_SCREG"
const val EV_kafkaBrokersOnPrem = "KAFKA_BROKERS_ON_PREM"

// Work vault dependencies
const val VAULT_initialLoad = "InitialLoad"
val kafkaSchemaReg = AnEnvironment.getEnvOrDefault(EV_kafkaSchemaReg, "http://localhost:8081")
val kafkaPersonTopic = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopic, "$PROGNAME-producer")
val kafkaPDLTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")

data class WorkSettings(
    val kafkaConsumerPerson: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
    ),
    val kafkaProducerPerson: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java
    ),
    val kafkaConsumerPdl: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg
    ),
    val kafkaConsumerPdlAlternative: Map<String, Any> = AKafkaConsumer.configAlternativeBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to AnEnvironment.getEnvOrDefault(EV_kafkaBrokersOnPrem, "Missing $EV_kafkaBrokersOnPrem")
    ),

    val initialLoad: Boolean = AVault.getSecretOrDefault(VAULT_initialLoad) == true.toString()
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

internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {
    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()

    var exitReason: ExitReason = ExitReason.NoKafkaProducer
    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                fromBeginning = false
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->
            workMetrics.noOfKakfaRecordsPdl.inc(cRecords.count().toDouble())
            // leaving if nothing to do
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

            exitReason = ExitReason.Work
            val results = cRecords.map { cr ->

                if (cr.value() == null) {
                    val personTombestone = PersonTombestone(aktoerId = cr.key())
                    workMetrics.noOfTombestone.inc()
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
                                    workMetrics.noOfPersonSf.inc()
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
                                workMetrics.publishedTombestones.inc()
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
            } else {
                KafkaConsumerStates.HasIssues
            }
        } // Consumer pdl topic
    } // Producer person topic

    log.info { "bootstrap work session finished" }

    if (!exitReason.isOK()) {
        log.error { "Work session has exited with not OK" }
    }

    return Pair(ws, exitReason)
}

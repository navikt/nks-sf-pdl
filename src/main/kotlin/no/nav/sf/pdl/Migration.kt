package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import java.io.File
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.KafkaConsumerStates
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs

private val log = KotlinLogging.logger {}

fun checkLatestFeed() {
    var retries = 5
    val kafkaConsumerGcpMigration: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
        // ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        // ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        "schema.registry.url" to kafkaSchemaReg,
        "security.protocol" to "SSL",
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
        ConsumerConfig.GROUP_ID_CONFIG to "nks-sf-pdl-migration-check",
        ConsumerConfig.CLIENT_ID_CONFIG to "nks-sf-pdl-migration-check"
    )

    val separateClientId = kafkaConsumerGcpMigration[ConsumerConfig.GROUP_ID_CONFIG] == "nks-sf-pdl-migration-check"

    if (!separateClientId) {
        log.error { "Abort check latest feed due to config not cascading as expected" }
        return
    }

    try {
        AKafkaConsumer<String, String?>(
            config = kafkaConsumerGcpMigration,
            topics = listOf("pdl.pdl-persondokument-v1"),
            fromBeginning = true
        ).consume { cRecords ->
            log.info { "Inside Migration Consumer" }
            if (cRecords.isEmpty) {
                if (workMetrics.recordsParsed.get().toInt() == 0 && retries > 0) {
                    log.info { "Migration: No records found $retries retries left, wait 60 w" }
                    retries--
                    Bootstrap.conditionalWait(60000)
                    return@consume KafkaConsumerStates.IsOk
                } else {
                    log.info { "Migration: No more records found (or given up) - end consume session" }
                    return@consume KafkaConsumerStates.IsFinished
                }
            }

            log.info { "Migration consumed batch of ${cRecords.count()}" }

            log.info { "Migration first ket ${cRecords.first().key()}" }

            val results = cRecords.map { cr ->
                if (cr.value() == null) {
                    val personTombestone = PersonTombestone(aktoerId = cr.key())
                    // workMetrics.tombstones.inc()
                    // Investigate.writeText("CONSUMED PERSON OFFSET ${cr.offset()} TOMBSTONE", true)
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
                                    Triple(KafkaConsumerStates.IsOk, personSf, cr.offset())
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
            log.info {
                "Migration check finished with ok flag $areOk count ${results.size} offsets ${
                    cRecords.first().offset()
                } to ${cRecords.last().offset()}"
            }
            File("/tmp/migrationcheck").writeText(
                "ok $areOk count ${results.size} offsets ${
                    cRecords.first().offset()
                } to ${cRecords.last().offset()}\nKeys processed:\n${cRecords.map { it.key() }.joinToString("\n")}"
            )
            KafkaConsumerStates.IsFinished
        }
    } catch (e: Exception) {
        log.error { "Migration consume attempt fail: ${e.message}" }
    }
    log.info { "Migration check exit" }
}

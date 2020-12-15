package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.EV_kafkaClientID
import no.nav.sf.library.PROGNAME
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

data class WorkSettings(
    val kafkaProducerGcp: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            "security.protocol" to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword)
    ),
    val kafkaConsumerGcp: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            "security.protocol" to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword)
    ),
    val kafkaConsumerOnPrem: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to fetchEnv(EV_kafkaBrokersOnPrem)
    ),
    val kafkaConsumerOnPremSeparateClientId: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            "schema.registry.url" to kafkaSchemaReg,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to fetchEnv(EV_kafkaBrokersOnPrem),
            ConsumerConfig.GROUP_ID_CONFIG to AnEnvironment.getEnvOrDefault(EV_kafkaClientID, PROGNAME) + "_init",
            ConsumerConfig.CLIENT_ID_CONFIG to AnEnvironment.getEnvOrDefault(EV_kafkaClientID, PROGNAME) + "_init"
    )
)

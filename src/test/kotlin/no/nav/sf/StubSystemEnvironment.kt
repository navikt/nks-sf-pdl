package no.nav.sf

import java.util.Properties
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.PrestopHook
import no.nav.sf.pdl.SystemEnvironment
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

class StubSystemEnvironment(
    val consumers: List<Consumer<*, *>>,
    val producers: List<Producer<*, *>>
) : SystemEnvironment() {

    private val consumerFn = sequence { consumers.forEach { yield({ _: Any -> it }) } }.iterator()
    private val producerFn = sequence { producers.forEach { yield({ _: Any -> it }) } }.iterator()

    override fun enableNAISAPIDelay() = 0L
    override fun consumeRecordRetryDelay() = 0L
    override fun bootstrapWaitTime() = 0L
    override fun workloopHook() = PrestopHook.activate() // Just short circuit the work loop after one run

    override fun <K, V> aKafkaConsumer(
        config: Map<String, Any>,
        fromBeginning: Boolean,
        topics: List<String>
    ) =
        AKafkaConsumer(
            config = config,
            retryConsumptionDelay = 0,
            fromBeginning = fromBeginning,
            topics = topics,
            kafkaConsumerFn = consumerFn.next() as ((Properties) -> Consumer<K, V>)
        )

    override fun <K, V> aKafkaConsumer(
        config: Map<String, Any>,
        fromBeginning: Boolean
    ) =
        AKafkaConsumer(
            config = config,
            retryConsumptionDelay = 0,
            fromBeginning = fromBeginning,
            kafkaConsumerFn = consumerFn.next() as ((Properties) -> Consumer<K, V>)
        )

    override fun <K, V> aKafkaProducer(
        config: Map<String, Any>
    ) =
        AKafkaProducer(
            config = config,
            kafkaProducerFn = producerFn.next() as ((Properties) -> Producer<K, V>)
        )
}

package no.nav.sf.pdl

import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer

class SystemEnvironment {

    open fun consumeRecordRetryDelay() = 60_000L

    open fun <K, V> aKafkaConsumer(
        config: Map<String, Any>,
        fromBeginning: Boolean,
        topics: List<String>
    ) =
        AKafkaConsumer<K, V>(config = config, fromBeginning = fromBeginning, topics = topics)

    open fun <K, V> aKafkaConsumer(
        config: Map<String, Any>,
        fromBeginning: Boolean
    ) =
        AKafkaConsumer<K, V>(config = config, fromBeginning = fromBeginning)

    open fun <K, V> aKafkaProducer(config: Map<String, Any>) =
        AKafkaProducer<K, V>(config = config)
}

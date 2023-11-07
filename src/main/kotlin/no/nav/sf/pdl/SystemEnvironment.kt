package no.nav.sf.pdl

import no.nav.sf.library.AKafkaConsumer

class SystemEnvironment {
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
}

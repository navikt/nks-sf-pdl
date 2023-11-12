package no.nav.sf

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.internals.ProduceRequestResult
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.Time

class TestValueWrapper() {
    private lateinit var producer: MockProducer<ByteArray, ByteArray?>
    private lateinit var cache: MutableMap<String, ByteArray?>
    private lateinit var key: String

    constructor(producer: MockProducer<ByteArray, ByteArray?>) : this() {
        this.producer = producer
    }

    constructor(cache: MutableMap<String, ByteArray?>) : this() {
        this.cache = cache
    }

    // Producer helpers
    fun containsKey(key: String) = (firstProducedKey(producer) ?: "").contains(key)
    fun hasKey(key: String) = this.apply { this.key = key }
    fun withValue(value: String?) = producedKey(key) && producedValue(value)
    fun butNotValue(value: String?) = producedKey(key) && producedValue(value).not()

    // Cache helpers
    fun hasCacheOnKey(key: String) = cache.containsKey(key)
    fun withCachedValue(value: String) = cacheAt(key).contains(value)
    fun withCachedNUllValue() = cacheAt(key).isEmpty()

    private fun producedKey(key: String) = firstProducedKey(producer)?.run { contains(key) } ?: false
    private fun producedValue(value: String?) =
        if (value == null)
            producedValue(producer) == null
        else
            producedValue(producer)?.contains(value) ?: false

    private fun firstProducedKey(producer: MockProducer<ByteArray, ByteArray?>) =
        firstRecord(producer)?.key()?.run { String(this) }

    private fun producedValue(producer: MockProducer<ByteArray, ByteArray?>) =
        firstRecord(producer)?.value()?.run { String(this) }

    private fun firstRecord(producer: MockProducer<ByteArray, ByteArray?>) = producer.history().firstOrNull()
    private fun cacheAt(key: String) = cache[key]?.run { String(this) } ?: ""
}

class TestProducer<K, V> : MockProducer<K, V>() {
    private val partition = 1
    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> {
        super.send(record)
        val topicPartition = TopicPartition(record.topic(), partition)
        val result = object : ProduceRequestResult(topicPartition) {
            override fun await() {}
            override fun baseOffset() = 1L
        }
        return FutureRecordMetadata(
            result, 0, 0, RecordBatch.NO_TIMESTAMP, 0, 0, Time.SYSTEM
        )
    }
}

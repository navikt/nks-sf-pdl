package no.nav.sf

import kotlinx.serialization.ImplicitReflectionSerializer
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.PrestopHook
import no.nav.sf.pdl.PersonSf
import no.nav.sf.pdl.Query
import no.nav.sf.pdl.getQueryFromJson
import no.nav.sf.pdl.gtCache
import no.nav.sf.pdl.kafkaGTTopic
import no.nav.sf.pdl.kafkaPersonTopic
import no.nav.sf.pdl.kafkaProducerTopicGt
import no.nav.sf.pdl.main
import no.nav.sf.pdl.personCache
import no.nav.sf.pdl.toPersonProto
import no.nav.sf.pdl.toPersonSf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ImplicitReflectionSerializer::class)
class BootstrapTest {
    private val partition = 1
    private val offset = 0L
    private lateinit var env: StubSystemEnvironment
    private lateinit var personCacheConsumer: MockConsumer<ByteArray, ByteArray?>
    private lateinit var gtCacheConsumer: MockConsumer<ByteArray, ByteArray?>
    private lateinit var gtConsumer: MockConsumer<String, String?>
    private lateinit var personConsumer: MockConsumer<String, String?>
    private lateinit var recordMetadata: RecordMetadata

    private lateinit var gtProducer: MockProducer<ByteArray, ByteArray?>
    private lateinit var appProducer: MockProducer<ByteArray, ByteArray?>

    @BeforeEach
    fun setUp() {
        recordMetadata = RecordMetadata(TopicPartition("topic", 1), 1, 1, 1, 1, 1, 1)
        gtCacheConsumer = MockConsumer(EARLIEST)
        personCacheConsumer = MockConsumer(EARLIEST)
        gtConsumer = MockConsumer(EARLIEST)
        personConsumer = MockConsumer(EARLIEST)
        gtProducer = TestProducer()
        appProducer = TestProducer()

        assignTopicToConsumer(cacheGTTopic(), gtCacheConsumer)
        assignTopicToConsumer(cachePersonTopic(), personCacheConsumer)
        subscribeTopicToConsumer(gtTopic(), gtConsumer)
        // Its super important that the sequence of consumer here represents the execution ordering of the Bootstrap - work loop
        env =
            StubSystemEnvironment(
                listOf(gtCacheConsumer, personCacheConsumer, gtConsumer, personConsumer, personConsumer),
                listOf(gtProducer, appProducer, appProducer)
            )
    }

    @AfterEach
    fun tearDown() {
        PrestopHook.reset()
        gtCache.clear()
        personCache.clear()
    }

    @Test
    fun `Bootstrap should load GT cache`() {
        // arrange
        val id = "myId"
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(id))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person())
        // act
        main(env)
        // assert
        assertTrue(`that`(gtCache).hasCacheOnKey(id))
    }

    @Test
    fun `Workloop consuming GT value that is not cached should add value to cache`() {
        // arrange
        setupDefaultCache()
        val id = "myId"
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue())
        // act
        main(env)
        // assert
        assertTrue(`that`(gtCache).hasCacheOnKey(id))
    }

    @Test
    fun `Workloop consuming GT value that is not cached should produce value`() {
        // arrange
        setupDefaultCache()
        val id = "myId"
        val value = "4321"
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue(value))
        // act
        main(env)
        // assert
        assertTrue(`that`(gtProducer).hasKey(id).withValue(value))
    }

    @Test
    fun `Workloop consuming GT value that is cached should not produce value`() {
        // arrange
        val id = "myId"
        setupDefaultCache(gtId = id)
        // adding
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue())
        // act
        main(env)
        // assert
        assertFalse(`that`(gtProducer).containsKey(id))
    }

    @Test
    fun `Workloop consuming GT value that is cached should produce value if value in GT is not equal consumed value`() {
        // arrange
        val id = "myId"
        val value = "4321"
        setupDefaultCache(gtId = id)
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue(value))
        // act
        main(env)
        // assert
        assertTrue(`that`(gtProducer).hasKey(id).withValue(value))
    }

    @Test
    fun `Workloop consuming GT value that is cached should change cached value`() {
        // arrange
        val id = "myId"
        val kommuneNr = "4321"
        setupDefaultCache(gtId = id)
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue(kommuneNr))
        // act
        main(env)
        // assert
        assertTrue(`that`(gtCache).hasKey(id).withCachedValue(kommuneNr))
    }

    @Test
    fun `Workloop consuming GT value that is cached should cache NULL value`() {
        // arrange
        val id = "myId"
        setupDefaultCache(gtId = id)
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = null)
        // act
        main(env)
        // assert
        assertTrue(`that`(gtCache).hasKey(id).withCachedNUllValue())
    }

    @Test
    fun `Workloop consuming GT value NULL with GT cached value NOT NULL should produce PERSON`() {
        // arrange
        val id = "myId"
        // Cache
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(id))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person(id))
        // changed GT value
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = null)
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).containsKey(id))
    }

    @Test
    fun `Workloop consuming GT value NULL with GT cached value NULL should NOT produce PERSON`() {
        // arrange
        val id = "myId"
        // Cache
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(id, isNull = true))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person(id))
        // changed GT value
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = null)
        // act
        main(env)
        // assert
        assertFalse(`that`(appProducer).containsKey(id))
    }

    @Test
    fun `Workloop consuming GT value NOT causing change to GT cache should NOT produce PERSON`() {
        // arrange
        val id = "myId"
        // Cache
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(id))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person(id))
        // changed GT value
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue())
        // act
        main(env)
        // assert
        assertFalse(`that`(appProducer).containsKey(id))
    }

    @Test
    fun `Workloop consuming GT value causing change to GT cache should produce PERSON with consumed GT value`() {
        // arrange
        val id = "myId"
        val changedKommuneNr = "9999"
        setupDefaultCache(gtId = id, personId = id)
        setupConsumerOnKeyAndValue(gtConsumer, key = id, value = gtValue(gtKommune = changedKommuneNr))
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).hasKey(id).withValue(changedKommuneNr))
    }

    @Test
    fun `Bootstrap should load PERSON cache`() {
        // arrange
        val id = "myId"
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt())
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person(id))
        // act
        main(env)
        // assert
        assertTrue(`that`(personCache).hasCacheOnKey(id))
    }

    @Test
    fun `Workloop consuming PERSON value should cache Person`() {
        // arrange
        setupDefaultCache()
        val id = "myId"
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = hentPerson(id))
        // act
        main(env)
        // assert
        assertTrue(`that`(personCache).hasCacheOnKey(id))
    }

    @Test
    fun `Workloop consuming PERSON value that is cached should not produce value`() {
        // arrange
        val id = "myId"
        setupDefaultCache(personId = id)
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = hentPerson(id))
        // act
        main(env)
        // assert
        assertFalse(`that`(appProducer).containsKey(id))
    }

    @Test
    fun `Workloop consuming PERSON value that is not cached should produce value`() {
        // arrange
        setupDefaultCache()
        val id = "myId"
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = hentPerson(id))
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).hasKey(id).withValue("bydelsnummerFraGt"))
    }

    @Test
    fun `Workloop consuming PERSON value that is GT cached with NULL value should produce changed value based on GT NUlL value`() {
        // arrange
        val id = "myId"
        val anotherPerson = hentPerson(id)
        // setup cache
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person())
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(id, isNull = true))
        // setup person consumption
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = anotherPerson)
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).hasKey(id).butNotValue("bydelsnummerFraGt"))
    }

    @Test
    fun `Workloop consuming PERSON value that is GT cached should produce changed value based on GT value`() {
        // arrange
        val id = "myId"
        val kommuneNr = "1732"
        val anotherPerson = hentPerson(id)
        // setup cache
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(aktoerId = id, kommunenummerFraGt = kommuneNr))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person())
        // setup person consumption
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = anotherPerson)
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).hasKey(id).withValue(kommuneNr))
    }

    @Test
    fun `Workloop consuming PERSON NULL value that is GT cached should produce PERSON NULL value`() {
        // arrange
        val id = "myId"
        setupDefaultCache(personId = id)
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = null)
        // act
        main(env)
        // assert
        assertTrue(`that`(appProducer).hasKey(id).withValue(null))
    }

    @Test
    fun `Workloop consuming PERSON NULL value that is GT cached should change cached GT value to NULL`() {
        // arrange
        val id = "myId"
        setupDefaultCache(personId = id)
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = null)
        // act
        main(env)
        // assert
        assertTrue(`that`(gtCache).hasKey(id).withCachedNUllValue())
    }

    @Test
    fun `Workloop consuming hollow PERSON should not produce PERSON`() {
        // arrange
        val id = "myId"
        setupDefaultCache()
        val anotherPerson = hentPerson(id, isHollow = true)
        setupConsumerOnKeyAndValue(personConsumer, key = id, value = anotherPerson)
        // act
        main(env)
        // assert
        assertFalse(`that`(appProducer).containsKey(id))
    }

    private fun `that`(producer: MockProducer<ByteArray, ByteArray?>) = TestValueWrapper(producer)
    private fun `that`(cache: MutableMap<String, ByteArray?>) = TestValueWrapper(cache)
    private fun gtTopic() = kafkaGTTopic
    private fun cachePersonTopic() = kafkaPersonTopic
    private fun cacheGTTopic() = kafkaProducerTopicGt

    private fun setupDefaultCache(gtId: String = "gtId", personId: String = "personId") {
        setupConsumerOnGtTopic(gtCacheConsumer, cacheGTTopic(), gt(gtId))
        setupConsumerOnPersonTopic(personCacheConsumer, cachePersonTopic(), person(personId))
    }

    private fun person(aktoerId: String = "aktoerId") =
        personSf(aktoerId).run {
            (this as PersonSf).toPersonProto()
        }

    private fun personSf(aktoerId: String = "aktoerId") =
        hentPerson(aktoerId)
            ?.getQueryFromJson()
            .run {
                (this as Query).toPersonSf()
            }

    private fun hentPerson(aktoerId: String, isHollow: Boolean = false) =
        resourceAsText(personPath(isHollow))
            ?.replace(""""ident": "AKTORID"""", """"ident": "$aktoerId"""")

    private fun personPath(isHollow: Boolean) =
        if (isHollow) "/pdlTopicValues/hollow.json" else "/pdlTopicValues/value.json"

    private fun setupConsumerOnPersonTopic(
        consumer: MockConsumer<ByteArray, ByteArray?>,
        topic: String,
        proto: Pair<PersonProto.PersonKey, PersonProto.PersonValue>
    ) =
        consumer.schedulePollTask {
            consumer.addRecord(asConsumerByteRecord(topic, proto))
        }

    private fun setupConsumerOnGtTopic(
        consumer: MockConsumer<ByteArray, ByteArray?>,
        topic: String,
        proto: Pair<PersonProto.PersonKey, PersonProto.Gt?>
    ) =
        consumer.schedulePollTask {
            consumer.addRecord(asConsumerByteRecordGt(topic, proto))
        }

    private fun setupConsumerOnKeyAndValue(
        consumer: MockConsumer<String, String?>,
        key: String,
        value: String?
    ) =
        consumer.schedulePollTask {
            val topicPartition = TopicPartition("topic", partition)
            consumer.rebalance(listOf(topicPartition))
            consumer.addRecord(asConsumerStringRecord("topic", key, value))
            consumer.updateBeginningOffsets(mapOf<TopicPartition, Long>(topicPartition to 0))
        }

    private fun asConsumerByteRecordGt(topic: String, proto: Pair<PersonProto.PersonKey, PersonProto.Gt?>) =
        ConsumerRecord<ByteArray, ByteArray?>(
            topic,
            partition,
            offset,
            proto.first.toByteArray(),
            proto.second?.toByteArray()
        )

    private fun asConsumerByteRecord(topic: String, proto: Pair<PersonProto.PersonKey, PersonProto.PersonValue>) =
        ConsumerRecord<ByteArray, ByteArray?>(
            topic,
            partition,
            offset,
            proto.first.toByteArray(),
            proto.second.toByteArray()
        )

    private fun asConsumerStringRecord(topic: String, key: String, value: String?) =
        ConsumerRecord<String, String?>(topic, partition, offset, key, value)

    private fun assignTopicToConsumer(topic: String, consumer: MockConsumer<ByteArray, ByteArray?>) {
        TopicPartition(topic, partition).run {
            updateConsumerPartitionOffset(this, consumer)
            updateConsumerPartition(this, consumer)
            consumer.assign(listOf(this))
        }
    }

    private fun subscribeTopicToConsumer(topic: String, consume: MockConsumer<String, String?>) =
        consume.subscribe(listOf(topic))

    private fun updateConsumerPartitionOffset(
        topicPartition: TopicPartition,
        consumer: MockConsumer<ByteArray, ByteArray?>
    ) =
        HashMap<TopicPartition, Long>().let { offsetMap ->
            offsetMap[topicPartition] = offset
            consumer.updateBeginningOffsets(offsetMap)
        }

    private fun updateConsumerPartition(
        topicPartition: TopicPartition,
        consumer: MockConsumer<ByteArray, ByteArray?>
    ) =
        consumer.updatePartitions(
            topicPartition.topic(), listOf(
                PartitionInfo(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    Node(1, "host", 1, "rack"),
                    emptyArray<Node>(),
                    emptyArray<Node>()
                )
            )
        )

    @ImplicitReflectionSerializer
    internal fun resourceAsText(path: String) =
        BootstrapTest::class.java.getResourceAsStream(path)?.bufferedReader().use { it?.readText() }

    fun gtValue(gtKommune: String = "1234") =
        """{ "geografiskTilknytning": {"gtType": "KOMMUNE", "gtKommune": "$gtKommune", "gtBydel": "bydelsnummerFraGt", "gtLand": "gtLand" }} """

    fun gt(
        aktoerId: String = "gtId",
        kommunenummerFraGt: String = "1234",
        bydelsnummerFraGt: String = "bydelsnummerFraGt",
        isNull: Boolean = false
    ) =
        personKey(aktoerId) to
                if (isNull) null else gtValue(kommunenummerFraGt, bydelsnummerFraGt)

    private fun gtValue(
        kommunenummerFraGt: String,
        bydelsnummerFraGt: String
    ): PersonProto.Gt? =
        PersonProto.Gt.newBuilder().apply {
            this.kommunenummerFraGt = kommunenummerFraGt
            this.bydelsnummerFraGt = bydelsnummerFraGt
        }.build()

    private fun personKey(aktoerId: String) =
        PersonProto.PersonKey.newBuilder().apply { this.aktoerId = aktoerId }.build()
}

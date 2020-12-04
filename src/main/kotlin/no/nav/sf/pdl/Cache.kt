package no.nav.sf.pdl

import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue

private val log = KotlinLogging.logger {}

val personCache: MutableMap<String, ByteArray?> = mutableMapOf()

val gtCache: MutableMap<String, ByteArray?> = mutableMapOf()

fun gtInitLoad(ws: WorkSettings) {
    workMetrics.clearAll()
    var gtTombestones = 0
    var gtSuccess = 0
    var gtFail = 0
    var gtRetries = 10

    log.info { "GT - test" }
    val kafkaConsumer = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdl,
            fromBeginning = true,
            topics = listOf(kafkaGTTopic)
    )
    // val investigate: MutableList<String> = mutableListOf()
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
                resultList.add(Pair(it.key(), null))
            } else {
                // if (investigate.size < 10) investigate.add(it.value() ?: "")
                it.value()?.let { v ->
                    when (val gt = v.getGtFromJson()) {
                        is Gt -> {
                            when (val gtValue = gt.toGtValue(it.key())) {
                                is GtValue -> {
                                    resultList.add(Pair(gtValue.aktoerId, gtValue.toGtProto().second.toByteArray()))
                                    gtSuccess++
                                }
                                else -> {
                                    log.error { "Gt parse error" }
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

    gtCache.putAll(resultList.toMap())
    log.info { "GT - test done tombestones $gtTombestones success $gtSuccess fails $gtFail" }

    log.info { "GT - resulting cache size ${gtCache.size} of which are tombstones ${gtCache.values.filter{it == null}.count()}" }
    var producerCount = 0
    gtCache.toList().asSequence().chunked(500000).forEach {
        log.info { "GT: Creating aiven producer for batch ${producerCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPersonAiven
        ).produce {
            it.fold(true) { acc, pair ->
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
    }

    log.info { "Done with gt load. Published gt: ${workMetrics.gtPublished.get().toInt()}, tombstones: ${workMetrics.gtPublishedTombstone.get().toInt()} p Issues: ${workMetrics.producerIssues.get().toInt()}" }
    // File("/tmp/investigategt").writeText("Findings:\n${investigate.joinToString("\n\n")}")
}

fun loadGtCache(ws: WorkSettings): ExitReason {
    log.info { "GT Cache - load" }
    val resultList: MutableList<Pair<String, ByteArray?>> = mutableListOf()
    var exitReason: ExitReason = ExitReason.NoKafkaConsumer
    val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray?>(
            config = ws.kafkaConsumerPersonAiven,
            fromBeginning = true,
            topics = listOf(kafkaProducerTopicGt)
    )
    kafkaConsumer.consume { consumerRecords ->
        exitReason = ExitReason.NoEvents
        if (consumerRecords.isEmpty) {
            if (workMetrics.gtCacheRecordsParsed.get().toInt() == 0) {
                log.info { "GT Cache - retry connection after waiting 60 s" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        exitReason = ExitReason.Work
        workMetrics.gtCacheRecordsParsed.inc(consumerRecords.count().toDouble())
        consumerRecords.forEach {
            GtBaseFromProto(it.key(), it.value()).also { gt ->
                when (gt) {
                    is GtProtobufIssue -> {
                        log.error { "GT Cache - Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
                        workMetrics.consumerIssues.inc()
                        return@consume KafkaConsumerStates.HasIssues
                    }
                    is GtInvalid -> {
                        log.error { "GT Cache - Protobuf parsing invalid person for offset ${it.offset()} in partition ${it.partition()}" }
                        workMetrics.consumerIssues.inc()
                        return@consume KafkaConsumerStates.HasIssues
                    }
                    is GtValue -> resultList.add(Pair(gt.aktoerId, it.value()))
                    is GtTombstone -> resultList.add(Pair(gt.aktoerId, null))
                }
            }
        }
        KafkaConsumerStates.IsOk
    }
    log.info { "GT Cache - Number of records from topic $kafkaProducerTopicGt is ${resultList.size}" }

    gtCache.putAll(resultList.toMap())

    log.info { "GT Cache - resulting cache size ${gtCache.size} of which are tombstones ${gtCache.values.filter{it == null}.count()}" }

    return exitReason
}

fun loadPersonCache(ws: WorkSettings): ExitReason {
    log.info { "Cache - load" }
    val resultList: MutableList<Pair<String, ByteArray?>> = mutableListOf()
    var exitReason: ExitReason = ExitReason.NoKafkaConsumer
    val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray?>(
            config = ws.kafkaConsumerPersonAiven,
            fromBeginning = true,
            topics = listOf(kafkaPersonTopic)
    )
    kafkaConsumer.consume { consumerRecords ->
        exitReason = ExitReason.NoEvents
        if (consumerRecords.isEmpty) {
            if (workMetrics.cacheRecordsParsed.get().toInt() == 0) {
                log.info { "Cache - retry connection after waiting 60 s " }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        exitReason = ExitReason.Work
        workMetrics.cacheRecordsParsed.inc(consumerRecords.count().toDouble())
        consumerRecords.forEach {
            PersonBaseFromProto(it.key(), it.value()).also { pb ->
                when (pb) {
                    is PersonProtobufIssue -> {
                        log.error { "Cache - Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
                        workMetrics.consumerIssues.inc()
                        return@consume KafkaConsumerStates.HasIssues
                    }
                    is PersonInvalid -> {
                        log.error { "Cache - Protobuf parsing invalid person for offset ${it.offset()} in partition ${it.partition()}" }
                        workMetrics.consumerIssues.inc()
                        return@consume KafkaConsumerStates.HasIssues
                    }
                    is PersonSf -> resultList.add(Pair(pb.aktoerId, it.value()))
                    is PersonTombestone -> resultList.add(Pair(pb.aktoerId, null))
                }
            }
        }
        KafkaConsumerStates.IsOk
    }
    log.info { "Cache - Number of records from topic $kafkaPersonTopic is ${resultList.size}" }

    personCache.putAll(resultList.toMap())

    log.info { "Cache - resulting cache size ${personCache.size} of which are tombstones ${personCache.values.filter{it == null}.count()}" }

    return exitReason
}

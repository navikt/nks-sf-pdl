package no.nav.sf.pdl

import java.io.File
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.KafkaConsumerStates

private val log = KotlinLogging.logger {}

val personCache: MutableMap<String, ByteArray> = mutableMapOf()

val gtCache: MutableMap<String, ByteArray> = mutableMapOf()

var gtTombestones = 0
var gtSuccess = 0
var gtFail = 0
fun gtTest(ws: WorkSettings) {
    workMetrics.clearAll()
    log.info { "GT - test" }
    val kafkaConsumer = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPersonAiven,
            fromBeginning = true,
            topics = listOf(kafkaGTTopic)
    )
    val investigate: MutableList<String> = mutableListOf()
    kafkaConsumer.consume { consumerRecords ->
        if (consumerRecords.isEmpty) {
            if (workMetrics.cacheRecordsParsed.get().toInt() == 0) {
                log.info { "Cache - retry connection after waiting 60 s" }
                Bootstrap.conditionalWait(60000)
                return@consume KafkaConsumerStates.IsOk
            }
            return@consume KafkaConsumerStates.IsFinished
        }
        consumerRecords.forEach {
            if (it.value() == null) {
                gtTombestones++
            } else {
                if (investigate.size < 10) investigate.add(it.value() ?: "")
                it.value()?.let {
                    when (val gt = it.getGtFromJson()) {
                        is Gt -> gtSuccess++
                        else -> gtFail++
                    }
                }
            }
        }
        KafkaConsumerStates.IsOk
    }
    log.info { "GT - test done tombestones $gtTombestones success $gtSuccess fails $gtFail" }

    File("/tmp/investigategt").writeText("Findings:\n${investigate.joinToString("\n\n")}")
}

fun loadPersonCache(ws: WorkSettings): ExitReason {
    log.info { "Cache - load" }
    val resultList: MutableList<Pair<String, ByteArray>> = mutableListOf()
    var exitReason: ExitReason = ExitReason.NoKafkaConsumer
    val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray>(
            config = ws.kafkaConsumerPersonAiven,
            fromBeginning = true,
            topics = listOf(kafkaPersonTopic)
    )
    kafkaConsumer.consume { consumerRecords ->
        exitReason = ExitReason.NoEvents
        if (consumerRecords.isEmpty) {
            if (workMetrics.cacheRecordsParsed.get().toInt() == 0) {
                log.info { "Cache - retry connection after waiting 60 s" }
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
                    is PersonTombestone -> resultList.add(Pair(pb.aktoerId, it.value()))
                }
            }
        }
        KafkaConsumerStates.IsOk
    }
    log.info { "Cache - Number of records from topic $kafkaPersonTopic is ${resultList.size}" }

    personCache.putAll(resultList.toMap())

    log.info { "Cache - resulting cache size ${personCache.size} " }

    if (resultList.size != personCache.size) {
        log.warn { "Cache - cache topic has duplicate keys!" }
    }

    return exitReason
}

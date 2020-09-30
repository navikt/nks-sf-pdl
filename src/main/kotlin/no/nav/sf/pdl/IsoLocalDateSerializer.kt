package no.nav.sf.pdl

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlinx.serialization.*

object IsoLocalDateSerializer : LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE)
open class LocalDateSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDate", PrimitiveKind.STRING) // StringDescriptor.withName(“java.time.LocalDate”)
    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.parse(decoder.decodeString(), formatter)
    }
    override fun serialize(encoder: Encoder, value: LocalDate) {
        encoder.encodeString(value.format(formatter))
    }
}
object IsoLocalDateTimeSerializer : LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
open class LocalDateTimeSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDateTime> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDateTime", PrimitiveKind.STRING) // StringDescriptor.withName(“java.time.LocalDateTime”)
    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), formatter)
    }
    override fun serialize(encoder: Encoder, value: LocalDateTime) {
        encoder.encodeString(value.format(formatter))
    }
}

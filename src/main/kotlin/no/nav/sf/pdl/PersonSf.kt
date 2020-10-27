package no.nav.sf.pdl

import com.google.protobuf.InvalidProtocolBufferException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.pdlsf.proto.PersonProto.PersonKey
import no.nav.pdlsf.proto.PersonProto.PersonValue

private val log = KotlinLogging.logger { }

@Serializable
data class Navn(
    val fornavn: String?,
    val mellomnavn: String?,
    val etternavn: String?
)

@Serializable
data class Adresser(
    val vegadresse: List<Vegadresse>,
    val matrikkeladresse: List<Matrikkeladresse>,
    val utenlandskAdresse: List<UtenlandskAdresse>,
    val ukjentBosted: List<UkjentBosted>
)

@Serializable
data class Vegadresse(
    val kommunenummer: String?,
    val adressenavn: String?,
    val husnummer: String?,
    val husbokstav: String?,
    val postnummer: String?,
    val bydelsnummer: String?,
    val koordinater: String?
)

@Serializable
data class Matrikkeladresse(
    val kommunenummer: String?,
    val postnummer: String?,
    val bydelsnummer: String?,
    val koordinater: String?
)

@Serializable
data class UkjentBosted(
    val bostedskommune: String?
)

@Serializable
data class UtenlandskAdresse(
    val adressenavnNummer: String?,
    val bygningEtasjeLeilighet: String?,
    val postboksNummerNavn: String?,
    val postkode: String?,
    val bySted: String?,
    val regionDistriktOmraade: String?,
    val landkode: String?
)

enum class AdresseType {
    VEGADRESSE,
    UKJENTBOSTED,
    UTENLANDSADRESSE
}

@Serializable
data class FamilieRelasjon(
    val relatertPersonsIdent: String?,
    val relatertPersonsRolle: String?,
    val minRolleForPerson: String?
)

@Serializable
data class UtflyttingFraNorge(
    val tilflyttingsland: String?,
    val tilflyttingsstedIUtlandet: String?
)

@Serializable
sealed class Adresse {
    @Serializable
    object Missing : Adresse()
    @Serializable
    object Invalid : Adresse()

    @Serializable
    data class Exist(
        val adresseType: AdresseType,
        val adresse: String?,
        val postnummer: String?,
        val kommunenummer: String?
    ) : Adresse()

    @Serializable
    data class Ukjent(
        val adresseType: AdresseType,
        val bostedsKommune: String?
    ) : Adresse()

    @Serializable
    data class Utenlands(
        val adresseType: AdresseType,
        val adresse: String?,
        val landkode: String
    ) : Adresse()
}

@Serializable
data class Sikkerhetstiltak(
    val beskrivelse: String?,
    val tiltaksType: String?,
    @Serializable(with = IsoLocalDateSerializer::class)
    val gyldigFraOgMed: LocalDate?,
    @Serializable(with = IsoLocalDateSerializer::class)
    val gyldigTilOgMed: LocalDate?,
    val kontaktpersonId: String?,
    val kontaktpersonEnhet: String?
)

@Serializable
data class Telefonnummer(
    val landskode: String?,
    val nummer: String?,
    val prioritet: Int
)

@Serializable
data class InnflyttingTilNorge(
    val fraflyttingsland: String?,
    val fraflyttingsstedIUtlandet: String?
)

@Serializable
data class Sivilstand(
    val type: String?,
    @Serializable(with = IsoLocalDateSerializer::class)
    val gyldigFraOgMed: LocalDate?,
    val relatertVedSivilstand: String?
)

@Serializable
data class Doedsfall(
    @Serializable(with = IsoLocalDateSerializer::class)
    val doedsdato: LocalDate?,
    val master: String?
)

@Serializable
data class PersonSf(
    val aktoerId: String, // OK
    val folkeregisterId: String, // OK
    val navn: List<Navn>,
    val familierelasjoner: List<FamilieRelasjon>, // OK
    val folkeregisterpersonstatus: List<String>, // OK
    val innflyttingTilNorge: List<InnflyttingTilNorge>, // OK
    val adressebeskyttelse: List<String>, // OK
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    val bostedsadresse: Adresser,
    val oppholdsadresse: Adresser,
    val statsborgerskap: List<String>,
    val sivilstand: List<Sivilstand>,
    val kommunenummerFraGt: String?,
    val kommunenummerFraAdresse: String?,
    val bydelsnummerFraGt: String?,
    val bydelsnummerFraAdresse: String?,
    val kjoenn: List<String>,
    val doedsfall: List<Doedsfall>,
    val telefonnummer: List<Telefonnummer>,
    val utflyttingFraNorge: List<UtflyttingFraNorge>,
    val talesspraaktolk: List<String>
) : PersonBase() {

    fun toJson(): String = no.nav.sf.library.json.stringify(serializer(), this)

    fun isDead(): Boolean = doedsfall.isNotEmpty()
}

internal fun ByteArray.protobufSafeParseKey(): PersonKey = this.let { ba ->
    try {
        PersonKey.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonKey.getDefaultInstance()
    }
}

internal fun ByteArray.protobufSafeParseValue(): PersonValue = this.let { ba ->
    try {
        PersonValue.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonValue.getDefaultInstance()
    }
}

sealed class PersonBase {
    companion object {
        fun createPersonTombstone(key: ByteArray): PersonBase =
                runCatching { PersonTombestone(PersonProto.PersonKey.parseFrom(key).aktoerId) }
                        .getOrDefault(PersonProtobufIssue)
    }
}

fun String?.toLocalDate(): LocalDate? =
        if (this == null || this == "") null else LocalDate.parse(this, DateTimeFormatter.ISO_DATE)

object PersonInvalid : PersonBase()
object PersonProtobufIssue : PersonBase()

data class PersonTombestone(
    val aktoerId: String
) : PersonBase() {
    fun toPersonTombstoneProtoKey(): PersonKey =
            PersonKey.newBuilder().apply {
                aktoerId = this@PersonTombestone.aktoerId
            }.build()
}

package no.nav.sf.pdl

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.pdlsf.proto.PersonProto.PersonKey

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
data class ForelderBarnRelasjon(
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
data class Fullmakt( // TODO Do not turn any enum repr. as strings to anything but strings
    val motpartsRolle: String?, // RolleDto
    val motpartsPersonident: String?,
    val omraader: List<String> = listOf(),
    @Serializable(with = IsoLocalDateSerializer::class)
    val gyldigFraOgMed: LocalDate? = null,
    @Serializable(with = IsoLocalDateSerializer::class)
    val gyldigTilOgMed: LocalDate? = null
)

@Serializable
data class VergemaalEllerFremtidsfullmakt(
    val type: String?,
    val embete: String?,
    val navn: Navn?,
    val motpartsPersonident: String?,
    val omfang: String?,
    val omfangetErInnenPersonligOmraade: Boolean?
)

@Serializable
data class PersonSf(
    val aktoerId: String,
    val folkeregisterId: List<String>,
    val navn: List<Navn>,
    var forelderBarnRelasjoner: List<ForelderBarnRelasjon>,
    val folkeregisterpersonstatus: List<String>,
    val innflyttingTilNorge: List<InnflyttingTilNorge>,
    val adressebeskyttelse: List<String>,
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    val bostedsadresse: Adresser,
    val oppholdsadresse: Adresser,
    val statsborgerskap: List<String>,
    val sivilstand: List<Sivilstand>,
    var kommunenummerFraGt: String,
    val kommunenummerFraAdresse: String,
    val bydelsnummerFraGt: String,
    val bydelsnummerFraAdresse: String,
    val kjoenn: List<String>,
    val doedsfall: List<Doedsfall>,
    val telefonnummer: List<Telefonnummer>,
    val utflyttingFraNorge: List<UtflyttingFraNorge>,
    val talesspraaktolk: List<String>,
    val fullmakt: List<Fullmakt>,
    val vergemaalEllerFremtidsfullmakt: List<VergemaalEllerFremtidsfullmakt>,
    val foedselsdato: List<String>
) : PersonBase() {

    fun toJson(): String = no.nav.sf.library.json.stringify(serializer(), this)

    fun isDead(): Boolean = doedsfall.isNotEmpty()
}

fun PersonSf.isHollowState(): Boolean = (navn.isEmpty() &&
        forelderBarnRelasjoner.isEmpty() && folkeregisterpersonstatus.isEmpty() &&
        innflyttingTilNorge.isEmpty() && adressebeskyttelse.isEmpty() &&
        sikkerhetstiltak.isEmpty() && bostedsadresse.utenlandskAdresse.isEmpty() &&
        bostedsadresse.ukjentBosted.isEmpty() && bostedsadresse.matrikkeladresse.isEmpty() &&
        bostedsadresse.vegadresse.isEmpty() && oppholdsadresse.utenlandskAdresse.isEmpty() &&
        oppholdsadresse.ukjentBosted.isEmpty() && oppholdsadresse.matrikkeladresse.isEmpty() &&
        oppholdsadresse.vegadresse.isEmpty() && statsborgerskap.isEmpty() && sivilstand.isEmpty() &&
        kjoenn.isEmpty() && doedsfall.isEmpty() && telefonnummer.isEmpty() && utflyttingFraNorge.isEmpty() &&
        talesspraaktolk.isEmpty() && fullmakt.isEmpty() && vergemaalEllerFremtidsfullmakt.isEmpty() && foedselsdato.isEmpty())

@Serializable
data class GtValue(
    val aktoerId: String,
    val kommunenummerFraGt: String,
    val bydelsnummerFraGt: String
) : GtValueBase()

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

sealed class GtValueBase {
    companion object {
        fun createGtTombstone(key: ByteArray): GtValueBase =
                runCatching { GtTombstone(PersonProto.PersonKey.parseFrom(key).aktoerId) }
                        .getOrDefault(GtProtobufIssue)
    }
}

object GtProtobufIssue : GtValueBase()
object GtInvalid : GtValueBase()

data class GtTombstone(
    val aktoerId: String
) : GtValueBase() {
    fun toPersonTombstoneProtoKey(): PersonKey =
            PersonKey.newBuilder().apply {
                aktoerId = this@GtTombstone.aktoerId
            }.build()
}

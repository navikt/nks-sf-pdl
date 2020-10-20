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

    /*
    fun toPersonProto(): Pair<PersonKey, PersonValue> =
            PersonKey.newBuilder().apply {
                aktoerId = this@PersonSf.aktoerId
            }.build() to PersonValue.newBuilder().apply {
                folkeregisterId = this@PersonSf.folkeregisterId
                fornavn = this@PersonSf.fornavn
                mellomnavn = this@PersonSf.mellomnavn
                etternavn = this@PersonSf.etternavn

                this@PersonSf.familierelasjoner.forEach { fr ->
                    addFamilierelasjoner(PersonProto.Familierelasjon.newBuilder().apply {
                        relatertPersonsIdent = fr.relatertPersonsIdent
                        relatertPersonsRolle = PersonProto.FamilieRelasjonsRolle.valueOf(fr.relatertPersonsRolle.name)
                        minRolleForPerson = fr.minRolleForPerson?.let { PersonProto.FamilieRelasjonsRolle.valueOf(it.name) }
                    }
                    )
                }

                folkeregisterpersonstatus = this@PersonSf.folkeregisterpersonstatus

                this@PersonSf.innflyttingTilNorge.forEach {
                    addInnflyttingTilNorge(PersonProto.InnflyttingTilNorge.newBuilder().apply {
                        fraflyttingsland = it.fraflyttingsland
                        fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet
                    }.build())
                }

                adressebeskyttelse = PersonProto.Gradering.valueOf(this@PersonSf.adressebeskyttelse.name)

                this@PersonSf.sikkerhetstiltak.forEach {
                    addSikkerhetstiltak(PersonProto.Sikkerhetstiltak.newBuilder().apply {
                        beskrivelse = it.beskrivelse
                        tiltakstype = PersonProto.Tiltakstype.valueOf(it.tiltaksType.name)
                        gyldigFraOgMed = it.gyldigFraOgMed?.format(DateTimeFormatter.ISO_DATE) ?: ""
                        gyldigTilOgMed = it.gyldigTilOgMed?.format(DateTimeFormatter.ISO_DATE) ?: ""
                        kontaktPersonId = it.kontaktpersonId
                        kontaktPersonEnhet = it.kontaktpersonEnhet
                    })
                }

                bostedsadresse = PersonProto.Adresse.newBuilder().apply {
                    val bostedsAdresse = this@PersonSf.bostedsadresse
                    if (bostedsAdresse is Adresse.Exist) {
                        type = PersonProto.AdresseType.valueOf(bostedsAdresse.adresseType.name)
                        adresse = bostedsAdresse.adresse
                        postnummer = bostedsAdresse.postnummer
                        kommunenummer = bostedsAdresse.kommunenummer
                    } else if (bostedsAdresse is Adresse.Ukjent) {
                        type = PersonProto.AdresseType.valueOf(AdresseType.UKJENTBOSTED.name)
                        bostedskommune = bostedsAdresse.bostedsKommune
                    }
                }.build()
                oppholdsadresse = PersonProto.Adresse.newBuilder().apply {
                    val oppholdsadresse = this@PersonSf.oppholdsadresse
                    if (oppholdsadresse is Adresse.Exist) {
                        type = PersonProto.AdresseType.valueOf(oppholdsadresse.adresseType.name)
                        adresse = oppholdsadresse.adresse
                        postnummer = oppholdsadresse.postnummer
                        kommunenummer = oppholdsadresse.kommunenummer
                    } else if (oppholdsadresse is Adresse.Utenlands) {
                        type = PersonProto.AdresseType.valueOf(AdresseType.UTENLANDSADRESSE.name)
                        adresse = oppholdsadresse.adresse
                        landkode = oppholdsadresse.landkode
                    }
                }.build()

                statsborgerskap = this@PersonSf.statsborgerskap

                this@PersonSf.sivilstand.forEach {
                    addSivilstand(PersonProto.Sivilstand.newBuilder().apply {
                        type = PersonProto.SivilstandType.valueOf(it.type.name)
                        gyldigFraOgMed = it.gyldigFraOgMed?.format(DateTimeFormatter.ISO_DATE) ?: ""
                        relatertVedSivilstand = it.relatertVedSivilstand
                    })
                }

                kommunenummeFraGt = this@PersonSf.kommunenummerFraGt

                kommunenummeFraAdresse = this@PersonSf.kommunenummerFraAdresse

                kjoenn = PersonProto.Kjoenn.valueOf(this@PersonSf.kjoenn.name)

                this@PersonSf.doedsfall.forEach {
                    addDoedsfall(PersonProto.Doedsfall.newBuilder().apply {
                        doedsdato = it.doedsdato?.format(DateTimeFormatter.ISO_DATE) ?: ""
                    })
                }

                this@PersonSf.telefonnummer.forEach { t -> addTelefonnummer(PersonProto.Telefonnummer.newBuilder().apply {
                    landkode = t.landskode
                    nummer = t.nummer
                    prioritet = t.prioritet
                }) }

                this@PersonSf.utflyttingFraNorge.forEach {
                    addUtflyttingFraNorge(PersonProto.UtflyttingFraNorge.newBuilder().apply {
                        tilflyttingsland = it.tilflyttingsland
                        tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet
                    })
                }

                this@PersonSf.talesspraaktolk.forEach { addTalesspraaktolk(it) }
            }
                    .build()*/

    fun toJson(): String = no.nav.sf.library.json.stringify(serializer(), this)
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
/*
        fun fromProto(key: ByteArray, value: ByteArray?): PersonBase =
                if (value == null) Companion.createPersonTombstone(key) else
                // runCatching {
                    PersonProto.PersonValue.parseFrom(value).let { v ->
                        PersonSf(
                                aktoerId = PersonKey.parseFrom(key).aktoerId,
                                folkeregisterId = v.folkeregisterId,
                                fornavn = v.fornavn,
                                mellomnavn = v.mellomnavn,
                                etternavn = v.etternavn,
                                familierelasjoner = v.familierelasjonerList.map { proto ->
                                    FamilieRelasjon(relatertPersonsIdent = proto.relatertPersonsIdent,
                                            relatertPersonsRolle = FamilieRelasjonsRolle.valueOf(proto.relatertPersonsRolle.name),
                                            minRolleForPerson = FamilieRelasjonsRolle.valueOf(proto.minRolleForPerson.name))
                                },
                                folkeregisterpersonstatus = v.folkeregisterpersonstatus,
                                innflyttingTilNorge = v.innflyttingTilNorgeList.map {
                                    InnflyttingTilNorge(fraflyttingsland = it.fraflyttingsland,
                                            fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet)
                                },
                                adressebeskyttelse = AdressebeskyttelseGradering.valueOf(v.adressebeskyttelse.name),
                                sikkerhetstiltak = v.sikkerhetstiltakList.map {
                                    Sikkerhetstiltak(beskrivelse = it.beskrivelse,
                                            tiltaksType = Tiltakstype.valueOf(it.tiltakstype.name),
                                            gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                            gyldigTilOgMed = it.gyldigTilOgMed.toLocalDate(),
                                            kontaktpersonId = it.kontaktPersonId,
                                            kontaktpersonEnhet = it.kontaktPersonEnhet)
                                },
                                bostedsadresse = Adresse.Exist(
                                        adresseType = AdresseType.valueOf(v.bostedsadresse.type.name),
                                        adresse = v.bostedsadresse.adresse,
                                        postnummer = v.bostedsadresse.postnummer,
                                        kommunenummer = v.bostedsadresse.kommunenummer
                                ),
                                oppholdsadresse = Adresse.Exist(
                                        adresseType = AdresseType.valueOf(v.oppholdsadresse.type.name),
                                        adresse = v.oppholdsadresse.adresse,
                                        postnummer = v.oppholdsadresse.postnummer,
                                        kommunenummer = v.oppholdsadresse.kommunenummer
                                ),
                                statsborgerskap = v.statsborgerskap,
                                sivilstand = v.sivilstandList.map {
                                    Sivilstand(
                                            type = Sivilstandstype.valueOf(it.type.name),
                                            gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                            relatertVedSivilstand = it.relatertVedSivilstand)
                                },
                                kommunenummerFraGt = v.kommunenummeFraGt,
                                kommunenummerFraAdresse = v.kommunenummeFraAdresse,
                                kjoenn = KjoennType.valueOf(v.kjoenn.name),
                                doedsfall = v.doedsfallList.map {
                                    Doedsfall(it.doedsdato.toLocalDate())
                                },
                                telefonnummer = v.telefonnummerList.map {
                                    Telefonnummer(
                                            landskode = it.landkode,
                                            nummer = it.nummer,
                                            prioritet = it.prioritet
                                    )
                                },
                                utflyttingFraNorge = v.utflyttingFraNorgeList.map {
                                    UtflyttingFraNorge(tilflyttingsland = it.tilflyttingsland,
                                            tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet)
                                },
                                talesspraaktolk = v.talesspraaktolkList
                        )
                    }*/
    }
    // }.getOrDefault(PersonProtobufIssue)
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

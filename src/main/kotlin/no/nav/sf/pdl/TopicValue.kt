package no.nav.sf.pdl

import java.time.LocalDate
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.sf.library.jsonNonStrict

private val log = KotlinLogging.logger { }

const val FREG = "FREG"
const val PDL = "PDL"

fun String.getQueryFromJson(): QueryBase = runCatching {
    jsonNonStrict.parse(Query.serializer(), this)
}
        .onFailure {
            log.error { "Cannot convert kafka value to query - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQuery)

@Serializable
enum class IdentGruppe {
    AKTORID,
    FOLKEREGISTERIDENT,
    NPID
}

@Serializable
enum class AdressebeskyttelseGradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}

@Serializable
enum class KjoennType {
    MANN,
    KVINNE,
    UKJENT
}

@Serializable
enum class GtType {
    KOMMUNE,
    BYDEL,
    UTLAND,
    UDEFINERT
}

@Serializable
enum class FamilieRelasjonsRolle {
    BARN,
    MOR,
    FAR,
    MORMOR
}

@Serializable
enum class Sivilstandstype {
    UOPPGITT,
    UGIFT,
    GIFT,
    ENKE_ELLER_ENKEMANN,
    SKILT,
    SEPARERT,
    REGISTRERT_PARTNER,
    SEPARERT_PARTNER,
    SKILT_PARTNER,
    GJENLEVENDE_PARTNE
}

@Serializable
data class Metadata(
    val historisk: Boolean = true,
    val master: String
)

sealed class QueryBase
object InvalidQuery : QueryBase()

@Serializable
data class Query(
    val hentPerson: Person,
    val hentIdenter: Identliste
) : QueryBase()

@Serializable
data class Identliste(
    val identer: List<IdentInformasjon>
) {
    @Serializable
    data class IdentInformasjon(
        val ident: String,
        val historisk: Boolean,
        val gruppe: IdentGruppe
    )
}

@Serializable
data class Person(
    val adressebeskyttelse: List<Adressebeskyttelse>,
    val bostedsadresse: List<Bostedsadresse>,
    val oppholdsadresse: List<Oppholdsadresse>,
    val doedsfall: List<Doedsfall>,
    var familieRelasjoner: List<FamilieRelasjon>,
    var innflyttingTilNorge: List<InnflyttingTilNorge>,
    val folkeregisterpersonstatus: List<Folkeregisterpersonstatus>,
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    var statsborgerskap: List<Statsborgerskap>,
    val sivilstand: List<Sivilstand>,
    val telefonnummer: List<Telefonnummer>,
    val kjoenn: List<Kjoenn>,
    val navn: List<Navn>,
    val geografiskTilknytning: GeografiskTilknytning? = null,
    val utflyttingFraNorge: List<UtflyttingFraNorge>
) {

    @Serializable
    data class Bostedsadresse(
        val vegadresse: Vegadresse?,
        val matrikkeladresse: Matrikkeladresse?,
        val ukjentBosted: UkjentBosted?,
        val metadata: Metadata
    ) {
        @Serializable
        data class Vegadresse(
            val kommunenummer: String?,
            val adressenavn: String?,
            val husnummer: String?,
            val husbokstav: String?,
            val postnummer: String?
        )

        @Serializable
        data class Matrikkeladresse(
            val kommunenummer: String?
        )

        @Serializable
        data class UkjentBosted(
            val bostedskommune: String?
        )
    }

    @Serializable
    data class Oppholdsadresse(
        val vegadresse: Vegadresse?,
        val utenlandsAdresse: UtenlandsAdresse?,
        val metadata: Metadata
    ) {

        @Serializable
        data class Vegadresse(
            val kommunenummer: String?,
            val adressenavn: String?,
            val husnummer: String?,
            val husbokstav: String?,
            val postnummer: String?
        )

        @Serializable
        data class UtenlandsAdresse(
            val adressenavnNummer: String?,
            val bygningEtasjeLeilighet: String?,
            val postboksNummerNavn: String?,
            val postkode: String?,
            val bySted: String?,
            val regionDistriktOmraade: String?,
            val landkode: String = ""
        )
    }

    @Serializable
    data class Doedsfall(
        val metadata: Metadata
    )

    @Serializable
    data class InnflyttingTilNorge(
        val fraflyttingsland: String = "",
        val fraflyttingsstedIUtlandet: String = "",
        val metadata: Metadata
    )

    @Serializable
    data class FamilieRelasjon(
        val relatertPersonsIdent: String = "",
        val relatertPersonsRolle: FamilieRelasjonsRolle,
        val minRolleForPerson: FamilieRelasjonsRolle?,
        val metadata: Metadata
    )

    @Serializable
    data class Sikkerhetstiltak(
        val beskrivelse: String,
        val metadata: Metadata
    )

    @Serializable
    data class Navn(
        val fornavn: String,
        val mellomnavn: String?,
        val etternavn: String,
        val metadata: Metadata
    )

    @Serializable
    data class Kjoenn(
        val kjoenn: KjoennType,
        val metadata: Metadata
    )

    @Serializable
    data class Statsborgerskap(
        val land: String?,
        val metadata: Metadata
    )

    @Serializable
    data class Sivilstand(
        val type: Sivilstandstype,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigFraOgMed: LocalDate? = null,
        val relatertVedSivilstand: String?,
        val metadata: Metadata
    )

    @Serializable
    data class Adressebeskyttelse(
        val gradering: AdressebeskyttelseGradering,
        val metadata: Metadata
    )

    @Serializable
    data class Folkeregisterpersonstatus(
        val status: String,
        val metadata: Metadata
    )

    @Serializable
    data class GeografiskTilknytning(
        val gtType: GtType,
        val gtKommune: String?,
        val gtBydel: String?,
        val gtLand: String?,
        val metadata: Metadata
    )

    @Serializable
    data class UtflyttingFraNorge(
        val tilflyttingsland: String = "",
        val tilflyttingsstedIUtlandet: String = "",
        val metadata: Metadata
    )

    @Serializable
    data class Telefonnummer(
        val landskode: String = "",
        val nummer: String = "",
        val prioritet: String = "",
        val metadata: Metadata
    )
}

internal const val UKJENT_FRA_PDL = "<UKJENT_FRA_PDL>"
fun Query.toPersonSf(): PersonBase {
    return runCatching {
        val kommunenummer = this.findGtKommunenummer()
        PersonSf(
                aktoerId = this.findAktoerId(),
                identifikasjonsnummer = this.findFolkeregisterIdent(),
                fornavn = this.findNavn().fornavn,
                mellomnavn = this.findNavn().mellomnavn,
                etternavn = this.findNavn().etternavn,
                familieRelasjon = this.findFamilieRelasjon(),
                folkeregisterpersonstatus = this.findFolkeregisterPersonStatus(),
                adressebeskyttelse = this.findAdressebeskyttelse(),
                innflyttingTilNorge = this.findInnflyttingTilNorge(),
                bostedsadresse = this.findBostedsAdresse(),
                oppholdsadresse = this.findOppholdsAdresse(),
                sikkerhetstiltak = this.hentPerson.sikkerhetstiltak.map { it.beskrivelse }.toList(),
                kommunenummer = kommunenummer,
                region = kommunenummer.regionOfKommuneNummer(),
                kjoenn = this.findKjoenn(),
                statsborgerskap = this.findStatsborgerskap(),
                sivilstand = this.findSivilstand(),
                telefonnummer = this.findTelefonnummer(),
                utflyttingFraNorge = this.findUtflyttingFraNorge(),
                doed = this.hentPerson.doedsfall.isNotEmpty() // "doedsdato": null  betyr at han faktsik er død, man vet bare ikke når. Listen kan ha to innslagt, kilde FREG og PDL
        )
    }
            .onFailure { log.error { "Error creating PersonSf from Query ${it.localizedMessage}" } }
            .getOrDefault(PersonInvalid)
}

private fun Query.findTelefonnummer(): TelefonnummerBase {
    return this.hentPerson.telefonnummer.let {
        telefonnummer ->

        if (telefonnummer.isEmpty()) {
            TelefonnummerBase.Missing
        } else {
            TelefonnummerBase.Exist(
                    telefonnummer
            )
        }
    }
}

private fun Query.findSivilstand(): Sivilstand {
    return this.hentPerson.sivilstand.let {
        sivilStand ->

        if (sivilStand.isEmpty()) {
            Sivilstand.Missing
        } else {
            sivilStand.firstOrNull { !it.metadata.historisk }?.let {
                sivilstand ->

                Sivilstand.Exist(
                    type = sivilstand.type,
                    gyldigFraOgMed = sivilstand.gyldigFraOgMed,
                    relatertVedSivilstand = sivilstand.relatertVedSivilstand
                )
            } ?: Sivilstand.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findInnflyttingTilNorge(): InnflyttingTilNorge {
    return this.hentPerson.innflyttingTilNorge.let {
        innflyttingTilNorge ->

        if (innflyttingTilNorge.isEmpty()) {
            InnflyttingTilNorge.Missing
        } else {
            innflyttingTilNorge.firstOrNull { !it.metadata.historisk }?.let {
                innflyttingTilNorge ->
                InnflyttingTilNorge.Exist(
                        fraflyttingsland = innflyttingTilNorge.fraflyttingsland,
                        fraflyttingsstedIUtlandet = innflyttingTilNorge.fraflyttingsstedIUtlandet
                )
            } ?: InnflyttingTilNorge.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findFolkeregisterPersonStatus(): String {
    return this.hentPerson.folkeregisterpersonstatus.let { folkeRegisterPersonStatus ->
        if (folkeRegisterPersonStatus.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            folkeRegisterPersonStatus.firstOrNull { !it.metadata.historisk }?.let {
                folkeRegisterPersonStatus ->

                folkeRegisterPersonStatus.status
            } ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findGtKommunenummer(): String {
    val kommunenr: Kommunenummer = this.hentPerson.geografiskTilknytning?.let { gt ->
        when (gt.gtType) {
            GtType.KOMMUNE -> {
                if (gt.gtKommune.isNullOrEmpty()) {
                    workMetrics.gtKommunenrFraKommuneMissing.inc()
                    Kommunenummer.Missing
                } else if ((gt.gtKommune.length == 4) || gt.gtKommune.all { c -> c.isDigit() }) {
                    workMetrics.gtKommunenrFraKommune.inc()
                    Kommunenummer.Exist(gt.gtKommune)
                } else {
                    workMetrics.gtKommuneInvalid.inc()
                    Kommunenummer.Invalid
                }
            }
            GtType.BYDEL -> {
                if (gt.gtBydel.isNullOrEmpty()) {
                    workMetrics.gtKommunenrFraBydelMissing.inc()
                    Kommunenummer.Missing
                } else if ((gt.gtBydel.length == 6) || gt.gtBydel.all { c -> c.isDigit() }) {
                    workMetrics.gtKommunenrFraBydel.inc()
                    Kommunenummer.Exist(gt.gtBydel.substring(0, 3))
                } else {
                    workMetrics.gtBydelInvalid.inc()
                    Kommunenummer.Invalid
                }
            }
            GtType.UTLAND -> {
                workMetrics.gtUtland.inc()
                Kommunenummer.GtUtland
            }
            GtType.UDEFINERT -> {
                workMetrics.gtUdefinert.inc()
                Kommunenummer.GtUdefinert
            }
        }
    } ?: workMetrics.gtMissing.inc().let { Kommunenummer.Missing }

    return if (kommunenr is Kommunenummer.Exist)
        kommunenr.knummer
    else {
        UKJENT_FRA_PDL
    }
}

private fun Query.findUtflyttingFraNorge(): UtflyttingFraNorge {
    return this.hentPerson.utflyttingFraNorge.let { utflyttingFraNorge ->
        if (utflyttingFraNorge.isEmpty()) {
            UtflyttingFraNorge.Missing
        } else {
            utflyttingFraNorge.firstOrNull { !it.metadata.historisk }?.let {
                utflyttingFraNorge ->
                UtflyttingFraNorge.Exist(
                        tilflyttingsland = utflyttingFraNorge.tilflyttingsland,
                        tilflyttingsstedIUtlandet = utflyttingFraNorge.tilflyttingsstedIUtlandet
                )
            } ?: UtflyttingFraNorge.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findStatsborgerskap(): String {
    return this.hentPerson.statsborgerskap.let { statsborgerskap ->
        if (statsborgerskap.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            statsborgerskap.joinToString {
                "\'${it.land}\'"
            }
        }
    }
}

private fun Query.findFamilieRelasjon(): FamilieRelasjon {
    return this.hentPerson.familieRelasjoner.let { familierelasjon ->
        if (familierelasjon.isEmpty()) {
            FamilieRelasjon.Missing
        } else {
            familierelasjon.firstOrNull { !it.metadata.historisk }?.let {
                familierelasjon ->
                FamilieRelasjon.Exist(
                        relatertPersonsIdent = familierelasjon.relatertPersonsIdent,
                        relatertPersonsRolle = familierelasjon.relatertPersonsRolle,
                        minRolleForPerson = familierelasjon.minRolleForPerson
                )
            } ?: FamilieRelasjon.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findOppholdsAdresse(): Adresse {
    return this.hentPerson.oppholdsadresse.let { oppholdsadresse ->
        if (oppholdsadresse.isEmpty()) {
            workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc()
            Adresse.Missing
        } else {
            oppholdsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegAdresse ->
                    Adresse.Exist(
                            adresseType = AdresseType.VEGADRESSE,
                            adresse = vegAdresse.adressenavn + " " + vegAdresse.husnummer + vegAdresse.husbokstav,
                            postnummer = vegAdresse.postnummer,
                            kommunenummer = vegAdresse.kommunenummer
                    )
                } ?: it.utenlandsAdresse?.let { utenlandskAdresse ->
                    Adresse.Utenlands(
                            adresseType = AdresseType.UTENLANDSADRESSE,
                            adresse =
                            utenlandskAdresse.adressenavnNummer + " " +
                                    utenlandskAdresse.bygningEtasjeLeilighet + " " +
                                    utenlandskAdresse.postboksNummerNavn + " " +
                                    utenlandskAdresse.postkode + " " +
                                    utenlandskAdresse.bySted + " " +
                                    utenlandskAdresse.regionDistriktOmraade,
                            landkode = utenlandskAdresse.landkode
                    )
                }
            } ?: Adresse.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findBostedsAdresse(): Adresse {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isEmpty()) {
            workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc()
            Adresse.Missing
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegAdresse ->
                    Adresse.Exist(
                            adresseType = AdresseType.VEGADRESSE,
                            adresse = vegAdresse.adressenavn + " " + vegAdresse.husnummer + vegAdresse.husbokstav,
                            postnummer = vegAdresse.postnummer,
                            kommunenummer = vegAdresse.kommunenummer
                    )
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    if (ukjentBosted.findKommuneNummer() is Kommunenummer.Exist) {
                        workMetrics.usedAddressTypes.labels(WMetrics.AddressType.UKJENTBOSTED.name).inc()
                        Adresse.Ukjent(
                                adresseType = AdresseType.UKJENTBOSTED,
                                bostedsKommune = ukjentBosted.bostedskommune)
                    } else Adresse.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.UKJENTBOSTED.name).inc() }
                }
            } ?: Adresse.Invalid.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

private fun Query.findAktoerId(): String {
    return this.hentIdenter.identer.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.AKTORID }?.ident ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findFolkeregisterIdent(): String {
    return this.hentIdenter.identer.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.FOLKEREGISTERIDENT }?.ident ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findAdressebeskyttelse(): AdressebeskyttelseGradering {
    return this.hentPerson.adressebeskyttelse.let { list ->
        if (list.isEmpty()) {
            AdressebeskyttelseGradering.UGRADERT
        } else {
            list.firstOrNull { !it.metadata.historisk }?.let { AdressebeskyttelseGradering.valueOf(it.gradering.name) }
                    ?: AdressebeskyttelseGradering.UGRADERT
        }
    }
}

private fun Query.findKjoenn(): KjoennType {
    return this.hentPerson.kjoenn.let { kjoenn ->
        if (kjoenn.isEmpty()) {
            KjoennType.UKJENT
        } else {
            kjoenn.firstOrNull { !it.metadata.historisk }?.let {
                KjoennType.valueOf(it.kjoenn.name)
            } ?: KjoennType.UKJENT
        }
    }
}

sealed class Kommunenummer {
    object Missing : Kommunenummer()
    object GtUtland : Kommunenummer()
    object GtUdefinert : Kommunenummer()
    object Invalid : Kommunenummer()

    data class Exist(val knummer: String) : Kommunenummer()
}

enum class AdresseType {
    VEGADRESSE,
    UKJENTBOSTED,
    UTENLANDSADRESSE
}

sealed class FamilieRelasjon {
    object Missing : FamilieRelasjon()
    object Invalid : FamilieRelasjon()

    data class Exist(
        val relatertPersonsIdent: String,
        val relatertPersonsRolle: FamilieRelasjonsRolle,
        val minRolleForPerson: FamilieRelasjonsRolle?
    ) : FamilieRelasjon()
}

sealed class UtflyttingFraNorge {
    object Missing : UtflyttingFraNorge()
    object Invalid : UtflyttingFraNorge()

    data class Exist(
        val tilflyttingsland: String,
        val tilflyttingsstedIUtlandet: String
    ) : UtflyttingFraNorge()
}

sealed class Adresse {
    object Missing : Adresse()
    object Invalid : Adresse()

    data class Exist(
        val adresseType: AdresseType,
        val adresse: String?,
        val postnummer: String?,
        val kommunenummer: String?
    ) : Adresse()

    data class Ukjent(
        val adresseType: AdresseType,
        val bostedsKommune: String?
    ) : Adresse()

    data class Utenlands(
        val adresseType: AdresseType,
        val adresse: String?,
        val landkode: String
    ) : Adresse()
}

sealed class InnflyttingTilNorge {
    object Missing : InnflyttingTilNorge()
    object Invalid : InnflyttingTilNorge()

    data class Exist(
        val fraflyttingsland: String,
        val fraflyttingsstedIUtlandet: String
    ) : InnflyttingTilNorge()
}

sealed class Sivilstand {
    object Missing : Sivilstand()
    object Invalid : Sivilstand()

    data class Exist(
        val type: Sivilstandstype,
        val gyldigFraOgMed: LocalDate?,
        val relatertVedSivilstand: String?
    ) : Sivilstand()
}

sealed class TelefonnummerBase {
    object Missing : TelefonnummerBase()
    object Invalid : TelefonnummerBase()

    data class Exist(
        val list: List<Person.Telefonnummer>
    ) : TelefonnummerBase()
}

fun Person.Bostedsadresse.Vegadresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        return Kommunenummer.Invalid
    }
}

fun Person.Bostedsadresse.Matrikkeladresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        return Kommunenummer.Invalid
    }
}

fun Person.Bostedsadresse.UkjentBosted.findKommuneNummer(): Kommunenummer {
    if (this.bostedskommune.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.bostedskommune.length == 4) || this.bostedskommune.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.bostedskommune)
    } else {
        workMetrics.noInvalidKommuneNummer.inc()
        workMetrics.invalidKommuneNummer.labels(this.bostedskommune).inc()
        return Kommunenummer.Invalid
    }
}

fun Query.findKommunenummer(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc()
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    if (vegadresse.findKommuneNummer() is Kommunenummer.Exist) {
                        workMetrics.usedAddressTypes.labels(WMetrics.AddressType.VEGAADRESSE.name).inc()
                        vegadresse.kommunenummer
                    } else null
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    if (matrikkeladresse.findKommuneNummer() is Kommunenummer.Exist) {
                        workMetrics.usedAddressTypes.labels(WMetrics.AddressType.MATRIKKELADRESSE.name).inc()
                        matrikkeladresse.kommunenummer
                    } else null
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    if (ukjentBosted.findKommuneNummer() is Kommunenummer.Exist) {
                        workMetrics.usedAddressTypes.labels(WMetrics.AddressType.UKJENTBOSTED.name).inc()
                        ukjentBosted.bostedskommune
                    } else null
                }
            } ?: UKJENT_FRA_PDL.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

fun String.regionOfKommuneNummer(): String {
    return if (this == UKJENT_FRA_PDL) this else this.substring(0, 2)
}

fun Query.findNavn(): NavnBase {
    return if (this.hentPerson.navn.isNullOrEmpty()) {
        NavnBase.Ukjent()
    } else {
        this.hentPerson.navn.firstOrNull { it.metadata.master.toUpperCase() == FREG && !it.metadata.historisk }?.let {
            if (it.etternavn.isNotBlank() && it.fornavn.isNotBlank())
                NavnBase.Freg(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
            else
                NavnBase.Ukjent(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
        }
                ?: this.hentPerson.navn.firstOrNull { it.metadata.master.toUpperCase() == PDL && !it.metadata.historisk }?.let {
                    if (it.etternavn.isNotBlank() && it.fornavn.isNotBlank())
                        NavnBase.Pdl(
                                fornavn = it.fornavn,
                                etternavn = it.etternavn,
                                mellomnavn = it.mellomnavn.orEmpty()
                        )
                    else
                        NavnBase.Ukjent(
                                fornavn = it.fornavn,
                                etternavn = it.etternavn,
                                mellomnavn = it.mellomnavn.orEmpty()
                        )
                } ?: NavnBase.Ukjent()
    }
}

sealed class NavnBase {
    abstract val fornavn: String
    abstract val mellomnavn: String
    abstract val etternavn: String

    data class Freg(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Pdl(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Ukjent(
        override val fornavn: String = UKJENT_FRA_PDL,
        override val mellomnavn: String = UKJENT_FRA_PDL,
        override val etternavn: String = UKJENT_FRA_PDL
    ) : NavnBase()
}

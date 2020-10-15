package no.nav.sf.pdl

import java.time.LocalDate
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

const val FREG = "FREG"
const val PDL = "PDL"

internal const val UKJENT_FRA_PDL = "<UKJENT_FRA_PDL>"
fun Query.toPersonSf(): PersonBase {
    return runCatching {
        PersonSf(
                aktoerId = this.findAktoerId(), // ok first !historisk from idents
                folkeregisterId = this.findFolkeregisterIdent(), // ok first !historisk from idents
                fornavn = this.findNavn().fornavn, // ok first !historisk from navn
                mellomnavn = this.findNavn().mellomnavn, // ok first !historisk from navn
                etternavn = this.findNavn().etternavn, // ok first !historisk from navn
                familierelasjoner = this.findFamilieRelasjoner(),
                folkeregisterpersonstatus = this.findFolkeregisterPersonStatus(),
                adressebeskyttelse = this.findAdressebeskyttelse(), // first !historisk
                innflyttingTilNorge = this.hentPerson.innflyttingTilNorge.map {
                    InnflyttingTilNorge(fraflyttingsland = it.fraflyttingsland,
                            fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet)
                },
                bostedsadresse = this.findBostedsAdresse(),
                oppholdsadresse = this.findOppholdsAdresse(), // TODO filter metadata.historisk på alla
                sikkerhetstiltak = this.hentPerson.sikkerhetstiltak.filter { !it.metadata.historisk }.map { hS ->
                    Sikkerhetstiltak(beskrivelse = hS.beskrivelse,
                            tiltaksType = hS.tiltakstype,
                            gyldigFraOgMed = hS.gyldigFraOgMed,
                            gyldigTilOgMed = hS.gyldigTilOgMed,
                            kontaktpersonId = hS.kontaktperson?.let { k -> k.personident } ?: UKJENT_FRA_PDL,
                            kontaktpersonEnhet = hS.kontaktperson?.let { k -> k.enhet } ?: UKJENT_FRA_PDL
                    )
                },
                kommunenummerFraGt = this.findGtKommunenummer(),
                kommunenummerFraAdresse = this.findAdresseKommunenummer(),
                //region = kommunenummer.regionOfKommuneNummer(), //Let SF do this
                kjoenn = this.findKjoenn(),
                statsborgerskap = this.findStatsborgerskap(),
                sivilstand = this.hentPerson.sivilstand.map {
                    Sivilstand(type = Sivilstandstype.valueOf(it.type.name),
                            gyldigFraOgMed = it.gyldigFraOgMed,
                            relatertVedSivilstand = it.relatertVedSivilstand)
                },
                telefonnummer = this.hentPerson.telefonnummer.filter { hTnr -> !hTnr.metadata.historisk }
                        .map { hTnr ->
                            Telefonnummer(landskode = hTnr.landskode, nummer = hTnr.nummer, prioritet = hTnr.prioritet)
                        },
                utflyttingFraNorge = this.hentPerson.utflyttingFraNorge.map {
                    UtflyttingFraNorge(tilflyttingsland = it.tilflyttingsland, tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet)
                },
                talesspraaktolk = this.hentPerson.tilrettelagtKommunikasjon.filter { it.talespraaktolk != null && !it.metadata.historisk && it.talespraaktolk.spraak != null }.map { it.talespraaktolk?.spraak ?: "" },
                doedsfall = this.hentPerson.doedsfall.map { Doedsfall(doedsdato = it.doedsdato) } // "doedsdato": null  betyr at han faktsik er død, man vet bare ikke når. Listen kan ha to innslagt, kilde FREG og PDL
        )
    }
            .onFailure { log.error { "Error creating PersonSf from Query ${it.localizedMessage}" } }
            .getOrDefault(PersonInvalid)
}

private fun Query.findFolkeregisterPersonStatus(): String {
    return this.hentPerson.folkeregisterpersonstatus.let { folkeRegisterPersonStatus ->
        if (folkeRegisterPersonStatus.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            folkeRegisterPersonStatus.firstOrNull { !it.metadata.historisk }?.let { folkeRegisterPersonStatus ->
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

fun Query.findAdresseKommunenummer(): String {
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

private fun Query.findStatsborgerskap(): String {
    return this.hentPerson.statsborgerskap.let { statsborgerskap ->
        if (statsborgerskap.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            statsborgerskap.joinToString {
                "${it.land}"
            }
        }
    }
}

private fun Query.findFamilieRelasjoner(): List<FamilieRelasjon> {
    return this.hentPerson.familierelasjoner.filter { fr -> !fr.metadata.historisk }.map { fr ->
        FamilieRelasjon(
                relatertPersonsIdent = fr.relatertPersonsIdent,
                relatertPersonsRolle = fr.relatertPersonsRolle,
                minRolleForPerson = fr.minRolleForPerson
        )
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
                } ?: it.utenlandskAdresse?.let { utenlandskAdresse ->
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
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.AKTORID && !it.historisk }?.ident
                    ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findFolkeregisterIdent(): String {
    return this.hentIdenter.identer.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.FOLKEREGISTERIDENT && !it.historisk }?.ident
                    ?: UKJENT_FRA_PDL
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

fun HentePerson.Bostedsadresse.Vegadresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.Matrikkeladresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.UkjentBosted.findKommuneNummer(): Kommunenummer {
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

fun Query.findDoedsdato(): LocalDate? {
    return this.hentPerson.doedsfall.firstOrNull { it.doedsdato != null }?.doedsdato
}

// fun String.regionOfKommuneNummer(): String {
//    return if (this == UKJENT_FRA_PDL) this else this.substring(0, 2)
// }

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

sealed class Kommunenummer {
    object Missing : Kommunenummer()
    object GtUtland : Kommunenummer()
    object GtUdefinert : Kommunenummer()
    object Invalid : Kommunenummer()

    data class Exist(val knummer: String) : Kommunenummer()
}

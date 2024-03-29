package no.nav.sf.pdl

import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils

private val log = KotlinLogging.logger { }

const val FREG = "FREG"
const val PDL = "PDL"

internal const val UKJENT_FRA_PDL = "<UKJENT_FRA_PDL>"
fun Query.toPersonSf(): PersonBase {
    return runCatching {
        PersonSf(
                identer = this.hentIdenter.identer,
                folkeregisteridentifikator = this.hentPerson.folkeregisteridentifikator,
                aktoerId = this.findAktoerId(), // ok first !historisk from idents
                folkeregisterId = this.findFolkeregisterIdent(), // !historisk from idents
                navn = this.hentPerson.navn.filter { !it.metadata!!.historisk }.map { Navn(fornavn = it.fornavn, mellomnavn = it.mellomnavn, etternavn = it.etternavn) },
                forelderBarnRelasjoner = this.findForelderBarnRelasjoner(),
                folkeregisterpersonstatus = this.hentPerson.folkeregisterpersonstatus.filter { !it.metadata.historisk }.map { it.status },
                adressebeskyttelse = this.hentPerson.adressebeskyttelse.filter { !it.metadata.historisk }.map { it.gradering.toString() }, // first !historisk
                innflyttingTilNorge = this.hentPerson.innflyttingTilNorge.filter { !it.metadata.historisk }.map {
                    InnflyttingTilNorge(fraflyttingsland = it.fraflyttingsland,
                            fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet)
                },
                bostedsadresse = this.findBostedsAdresse(),
                oppholdsadresse = this.findOppholdsAdresse(),
                sikkerhetstiltak = this.hentPerson.sikkerhetstiltak.filter { !it.metadata.historisk }.map { hS ->
                    Sikkerhetstiltak(beskrivelse = hS.beskrivelse,
                            tiltaksType = hS.tiltakstype.toString(),
                            gyldigFraOgMed = hS.gyldigFraOgMed,
                            gyldigTilOgMed = hS.gyldigTilOgMed,
                            kontaktpersonId = hS.kontaktperson?.personident ?: UKJENT_FRA_PDL,
                            kontaktpersonEnhet = hS.kontaktperson?.enhet ?: UKJENT_FRA_PDL
                    )
                },
                kommunenummerFraGt = this.findGtKommunenummer(),
                kommunenummerFraAdresse = this.findAdresseKommunenummer(),
                bydelsnummerFraGt = this.findGtBydelsnummer(),
                bydelsnummerFraAdresse = this.findAdresseBydelsnummer(),
                kjoenn = this.hentPerson.kjoenn.filter { !it.metadata.historisk }.map { it.kjoenn.name },
                statsborgerskap = this.hentPerson.statsborgerskap.filter { !it.metadata.historisk && it.land != null }.map { it.land ?: "" },
                sivilstand = this.hentPerson.sivilstand.filter { !it.metadata.historisk }.map {
                    Sivilstand(type = Sivilstandstype.valueOf(it.type.name).toString(),
                            gyldigFraOgMed = it.gyldigFraOgMed,
                            relatertVedSivilstand = it.relatertVedSivilstand)
                },
                telefonnummer = this.hentPerson.telefonnummer.filter { hTnr -> !hTnr.metadata.historisk }
                        .map { hTnr ->
                            Telefonnummer(landskode = hTnr.landskode, nummer = hTnr.nummer, prioritet = hTnr.prioritet)
                        },
                utflyttingFraNorge = this.hentPerson.utflyttingFraNorge.filter { !it.metadata.historisk }.map {
                    UtflyttingFraNorge(tilflyttingsland = it.tilflyttingsland, tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet)
                },
                talesspraaktolk = this.hentPerson.tilrettelagtKommunikasjon.filter { it.talespraaktolk != null && !it.metadata.historisk && it.talespraaktolk.spraak != null }.map {
                    it.talespraaktolk?.spraak ?: ""
                },
                doedsfall = this.hentPerson.doedsfall.filter { !it.metadata.historisk }.map { Doedsfall(doedsdato = it.doedsdato, master = it.metadata.master) }, // "doedsdato": null  betyr at han faktsik er død, man vet bare ikke når. Listen kan ha to innslagt, kilde FREG og PDL
                fullmakt = this.hentPerson.fullmakt.filter { !it.metadata.historisk }.map {
                    Fullmakt(motpartsRolle = it.motpartsRolle, motpartsPersonident = it.motpartsPersonident, omraader = it.omraader, gyldigFraOgMed = it.gyldigFraOgMed, gyldigTilOgMed = it.gyldigTilOgMed)
                },
                vergemaalEllerFremtidsfullmakt = this.hentPerson.vergemaalEllerFremtidsfullmakt.filter { !it.metadata.historisk }.map {
                    VergemaalEllerFremtidsfullmakt(type = it.type, embete = it.embete,
                            navn = it.vergeEllerFullmektig.navn?.let { Navn(fornavn = it.fornavn, mellomnavn = it.mellomnavn, etternavn = it.etternavn) },
                            motpartsPersonident = it.vergeEllerFullmektig.motpartsPersonident, omfang = it.vergeEllerFullmektig.omfang, omfangetErInnenPersonligOmraade = it.vergeEllerFullmektig.omfangetErInnenPersonligOmraade)
                },
                foedselsdato = this.hentPerson.foedsel.filter { !it.metadata.historisk }.map { it.foedselsdato ?: "" }
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
            folkeRegisterPersonStatus.firstOrNull { !it.metadata.historisk }?.status ?: UKJENT_FRA_PDL
        }
    }
}

fun HentePerson.GeografiskTilknytning?.findGtKommunenummer(): String {
    val kommunenr: Kommunenummer = this?.let { gt ->
        when (gt.gtType) {
            GtType.KOMMUNE -> {
                if (gt.gtKommune.isNullOrEmpty()) {
                    Kommunenummer.Missing
                } else if ((gt.gtKommune.length == 4) || gt.gtKommune.all { c -> c.isDigit() }) {
                    Kommunenummer.Exist(gt.gtKommune)
                } else {
                    Kommunenummer.Invalid
                }
            }
            GtType.BYDEL -> {
                if (gt.gtBydel.isNullOrEmpty()) {
                    Kommunenummer.Missing
                } else if ((gt.gtBydel.length == 6) || gt.gtBydel.all { c -> c.isDigit() }) {
                    Kommunenummer.Exist(gt.gtBydel.substring(0, 4))
                } else {
                    Kommunenummer.Invalid
                }
            }
            GtType.UTLAND -> {
                Kommunenummer.GtUtland
            }
            GtType.UDEFINERT -> {
                Kommunenummer.GtUdefinert
            }
        }
    } ?: Kommunenummer.Missing

    return if (kommunenr is Kommunenummer.Exist)
        kommunenr.knummer
    else {
        UKJENT_FRA_PDL
    }
}

fun HentePerson.GeografiskTilknytning?.findGtBydelsnummer(): String =
    this?.let { gt ->
        if (!gt.gtBydel.isNullOrEmpty()) {
            gt.gtBydel
        } else {
            UKJENT_FRA_PDL
        } } ?: UKJENT_FRA_PDL

private fun Query.findGtKommunenummer(): String {
    return this.hentPerson.geografiskTilknytning.findGtKommunenummer()
}

private fun Query.findGtBydelsnummer(): String =
    this.hentPerson.geografiskTilknytning.findGtBydelsnummer()

fun Query.findAdresseKommunenummer(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    if (vegadresse.findKommuneNummer() is Kommunenummer.Exist) {
                        vegadresse.kommunenummer
                    } else null
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    if (matrikkeladresse.findKommuneNummer() is Kommunenummer.Exist) {
                        matrikkeladresse.kommunenummer
                    } else null
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    if (ukjentBosted.findKommuneNummer() is Kommunenummer.Exist) {
                        ukjentBosted.bostedskommune
                    } else null
                }
            } ?: UKJENT_FRA_PDL
        }
    }
}

fun Query.findAdresseBydelsnummer(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    vegadresse.bydelsnummer ?: UKJENT_FRA_PDL
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    matrikkeladresse.bydelsnummer ?: UKJENT_FRA_PDL
                } ?: it.ukjentBosted?.let {
                    UKJENT_FRA_PDL
                }
            } ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findForelderBarnRelasjoner(): List<ForelderBarnRelasjon> {
    return this.hentPerson.forelderBarnRelasjon.filter { fbr -> !fbr.metadata.historisk }.map { fbr ->
        ForelderBarnRelasjon(
                relatertPersonsIdent = fbr.relatertPersonsIdent,
                relatertPersonsRolle = fbr.relatertPersonsRolle.toString(),
                minRolleForPerson = fbr.minRolleForPerson.toString()
        )
    }
}

fun HentePerson.Bostedsadresse.Koordinater.toKoordinaterString(): String? =
        if (this.x == null || this.y == null) {
            null
        } else {
            "${this.x},${this.y},${this.z ?: 0}"
        }

fun HentePerson.Oppholdsadresse.Koordinater.toKoordinaterString(): String? =
        if (this.x == null || this.y == null) {
            null
        } else {
            "${this.x},${this.y},${this.z ?: 0}"
        }

private fun Query.findBostedsAdresse(): Adresser {
    return Adresser(
            vegadresse = this.hentPerson.bostedsadresse.filter { it.vegadresse != null && !it.metadata.historisk }.map { it.vegadresse }
                    .map {
                        Vegadresse(kommunenummer = it?.kommunenummer,
                                adressenavn = it?.adressenavn,
                                husnummer = it?.husnummer,
                                husbokstav = it?.husbokstav,
                                postnummer = it?.postnummer,
                                bydelsnummer = it?.bydelsnummer,
                                koordinater = it?.koordinater?.toKoordinaterString())
                    },
            matrikkeladresse = this.hentPerson.bostedsadresse.filter { it.matrikkeladresse != null && !it.metadata.historisk }.map { it.matrikkeladresse }
                    .map {
                        Matrikkeladresse(kommunenummer = it?.kommunenummer,
                                postnummer = it?.postnummer,
                                bydelsnummer = it?.bydelsnummer,
                                koordinater = it?.koordinater?.toKoordinaterString())
                    },
            ukjentBosted = this.hentPerson.bostedsadresse.filter { it.ukjentBosted != null && !it.metadata.historisk }.map { it.ukjentBosted }
                    .map {
                        UkjentBosted(bostedskommune = it?.bostedskommune)
                    },
            utenlandskAdresse = this.hentPerson.bostedsadresse.filter { it.utenlandskAdresse != null && !it.metadata.historisk }.map { it.utenlandskAdresse }
                    .map {
                        UtenlandskAdresse(
                                adressenavnNummer = it?.adressenavnNummer,
                                bygningEtasjeLeilighet = it?.bygningEtasjeLeilighet,
                                postboksNummerNavn = it?.postboksNummerNavn,
                                postkode = it?.postkode,
                                bySted = it?.bySted,
                                regionDistriktOmraade = it?.regionDistriktOmraade,
                                landkode = it?.landkode
                        )
                    }
    )
}

private fun Query.findOppholdsAdresse(): Adresser {
    return Adresser(
            vegadresse = this.hentPerson.oppholdsadresse.filter { it.vegadresse != null && !it.metadata.historisk }.map { it.vegadresse }
                    .map {
                        Vegadresse(kommunenummer = it?.kommunenummer,
                                adressenavn = it?.adressenavn,
                                husnummer = it?.husnummer,
                                husbokstav = it?.husbokstav,
                                postnummer = it?.postnummer,
                                bydelsnummer = it?.bydelsnummer,
                                koordinater = it?.koordinater?.toKoordinaterString())
                    },
            matrikkeladresse = this.hentPerson.oppholdsadresse.filter { it.matrikkeladresse != null && !it.metadata.historisk }.map { it.matrikkeladresse }
                    .map {
                        Matrikkeladresse(kommunenummer = it?.kommunenummer,
                                postnummer = it?.postnummer,
                                bydelsnummer = it?.bydelsnummer,
                                koordinater = it?.koordinater?.toKoordinaterString())
                    },
            ukjentBosted = emptyList(),
            utenlandskAdresse = this.hentPerson.oppholdsadresse.filter { it.utenlandskAdresse != null && !it.metadata.historisk }.map { it.utenlandskAdresse }
                    .map {
                        UtenlandskAdresse(
                                adressenavnNummer = it?.adressenavnNummer,
                                bygningEtasjeLeilighet = it?.bygningEtasjeLeilighet,
                                postboksNummerNavn = it?.postboksNummerNavn,
                                postkode = it?.postkode,
                                bySted = it?.bySted,
                                regionDistriktOmraade = it?.regionDistriktOmraade,
                                landkode = it?.landkode
                        )
                    }
    )
}

private fun Query.findAktoerId(): String {
    return this.hentIdenter.identer.filter { it.gruppe == IdentGruppe.AKTORID.toString() && !it.historisk }.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            if (it.size > 1) {
                log.error { "More then one non-historic aktoersid found: ${StringUtils.join(it, ",")}" }
            }
            it.firstOrNull()?.ident ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findFolkeregisterIdent(): List<String> {
    return this.hentIdenter.identer.filter { it.gruppe == IdentGruppe.FOLKEREGISTERIDENT.toString() && !it.historisk }.map { it.ident }
}

fun HentePerson.Bostedsadresse.Vegadresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        log.error { "Found invalid kommunenummer ${this.kommunenummer}" }
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.Matrikkeladresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        log.error { "Found invalid kommunenummer ${this.kommunenummer}" }
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.UkjentBosted.findKommuneNummer(): Kommunenummer {
    if (this.bostedskommune.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.bostedskommune.length == 4) || this.bostedskommune.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.bostedskommune)
    } else {
        log.error { "Invalid kommunenr found: ${this.bostedskommune}" }
        return Kommunenummer.Invalid
    }
}

sealed class Kommunenummer {
    object Missing : Kommunenummer()
    object GtUtland : Kommunenummer()
    object GtUdefinert : Kommunenummer()
    object Invalid : Kommunenummer()

    data class Exist(val knummer: String) : Kommunenummer()
}

package no.nav.sf.pdl

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import no.nav.pdlsf.proto.PersonProto

fun LocalDate?.toIsoString(): String {
    return this?.format(DateTimeFormatter.ISO_DATE) ?: ""
}

fun PersonSf.toPersonProto(): Pair<PersonProto.PersonKey, PersonProto.PersonValue> =
        this.let {
            PersonProto.PersonKey.newBuilder().apply {
                aktoerId = it.aktoerId
            }.build() to PersonProto.PersonValue.newBuilder().apply {
                it.folkeregisterId.forEach {
                    addFolkeregisterId(it)
                }

                it.navn.forEach {
                    addNavn(PersonProto.Navn.newBuilder().apply {
                        fornavn = it.fornavn ?: ""
                        mellomnavn = it.mellomnavn ?: ""
                        etternavn = it.etternavn ?: ""
                    })
                }

                it.familierelasjoner.forEach { fr ->
                    addFamilierelasjoner(PersonProto.Familierelasjon.newBuilder().apply {
                        relatertPersonsIdent = fr.relatertPersonsIdent ?: ""
                        relatertPersonsRolle = fr.relatertPersonsRolle ?: ""
                        minRolleForPerson = fr.minRolleForPerson ?: ""
                    }.build())
                }

                it.folkeregisterpersonstatus.forEach {
                    addFolkeregisterpersonstatus(it ?: "")
                }

                it.innflyttingTilNorge.forEach {
                    addInnflyttingTilNorge(PersonProto.InnflyttingTilNorge.newBuilder().apply {
                        fraflyttingsland = it.fraflyttingsland ?: ""
                        fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet ?: ""
                    }.build())
                }

                it.adressebeskyttelse.forEach {
                    addAdressebeskyttelse(it)
                }

                it.sikkerhetstiltak.forEach {
                    addSikkerhetstiltak(PersonProto.Sikkerhetstiltak.newBuilder().apply {
                        beskrivelse = it.beskrivelse ?: ""
                        tiltakstype = it.tiltaksType ?: ""
                        gyldigFraOgMed = it.gyldigFraOgMed.toIsoString()
                        gyldigTilOgMed = it.gyldigTilOgMed.toIsoString()
                        kontaktpersonId = it.kontaktpersonId ?: ""
                        kontaktpersonEnhet = it.kontaktpersonEnhet ?: ""
                    }.build())
                }

                bostedsadresse = PersonProto.Adresser.newBuilder().apply {
                    it.bostedsadresse.vegadresse.forEach {
                        addVegadresse(PersonProto.Vegadresse.newBuilder().apply {
                            kommunenummer = it.kommunenummer ?: ""
                            adressenavn = it.adressenavn ?: ""
                            husnummer = it.husnummer ?: ""
                            husbokstav = it.husbokstav ?: ""
                            postnummer = it.postnummer ?: ""
                            bydelsnummer = it.bydelsnummer ?: ""
                            koordinater = it.koordinater ?: ""
                        })
                    }
                    it.bostedsadresse.matrikkeladresse.forEach {
                        addMatrikkeladresse(PersonProto.Matrikkeladresse.newBuilder().apply {
                            kommunenummer = it.kommunenummer ?: ""
                            postnummer = it.postnummer ?: ""
                            bydelsnummer = it.bydelsnummer ?: ""
                            koordinater = it.koordinater ?: ""
                        })
                    }
                    it.bostedsadresse.ukjentBosted.forEach {
                        addUkjentBosted(PersonProto.UkjentBosted.newBuilder().apply {
                            bostedskommune = it.bostedskommune ?: ""
                        })
                    }
                    it.bostedsadresse.utenlandskAdresse.forEach {
                        addUtenlandskAdresse(PersonProto.UtenlandskAdresse.newBuilder().apply {
                            adressenavnNummer = it.adressenavnNummer ?: ""
                            bygningEtasjeLeilighet = it.bygningEtasjeLeilighet ?: ""
                            postboksNummerNavn = it.postboksNummerNavn ?: ""
                            postkode = it.postkode ?: ""
                            bySted = it.bySted ?: ""
                            regionDistriktOmraade = it.regionDistriktOmraade ?: ""
                            landkode = it.landkode ?: ""
                        })
                    }
                }.build()

                oppholdsadresse = PersonProto.Adresser.newBuilder().apply {
                    it.oppholdsadresse.vegadresse.forEach {
                        addVegadresse(PersonProto.Vegadresse.newBuilder().apply {
                            kommunenummer = it.kommunenummer ?: ""
                            adressenavn = it.adressenavn ?: ""
                            husnummer = it.husnummer ?: ""
                            husbokstav = it.husbokstav ?: ""
                            postnummer = it.postnummer ?: ""
                            bydelsnummer = it.bydelsnummer ?: ""
                            koordinater = it.koordinater ?: ""
                        })
                    }
                    it.oppholdsadresse.matrikkeladresse.forEach {
                        addMatrikkeladresse(PersonProto.Matrikkeladresse.newBuilder().apply {
                            kommunenummer = it.kommunenummer ?: ""
                            postnummer = it.postnummer ?: ""
                            bydelsnummer = it.bydelsnummer ?: ""
                            koordinater = it.koordinater ?: ""
                        })
                    }
                    it.oppholdsadresse.ukjentBosted.forEach {
                        addUkjentBosted(PersonProto.UkjentBosted.newBuilder().apply {
                            bostedskommune = it.bostedskommune ?: ""
                        })
                    }
                    it.oppholdsadresse.utenlandskAdresse.forEach {
                        addUtenlandskAdresse(PersonProto.UtenlandskAdresse.newBuilder().apply {
                            adressenavnNummer = it.adressenavnNummer ?: ""
                            bygningEtasjeLeilighet = it.bygningEtasjeLeilighet ?: ""
                            postboksNummerNavn = it.postboksNummerNavn ?: ""
                            postkode = it.postkode ?: ""
                            bySted = it.bySted ?: ""
                            regionDistriktOmraade = it.regionDistriktOmraade ?: ""
                            landkode = it.landkode ?: ""
                        })
                    }
                }.build()

                it.statsborgerskap.forEach { addStatsborgerskap(it ?: "") }

                it.sivilstand.forEach {
                    addSivilstand(PersonProto.Sivilstand.newBuilder().apply {
                        type = it.type ?: ""
                        gyldigFraOgMed = it.gyldigFraOgMed.toIsoString()
                        relatertVedSivilstand = it.relatertVedSivilstand ?: ""
                    })
                }

                kommunenummerFraGt = it.kommunenummerFraGt

                bydelsnummerFraGt = it.bydelsnummerFraGt

                kommunenummerFraAdresse = it.kommunenummerFraAdresse

                bydelsnummerFraAdresse = it.bydelsnummerFraAdresse

                it.kjoenn.forEach {
                    addKjoenn(it ?: "")
                }

                it.doedsfall.forEach {
                    addDoedsfall(PersonProto.Doedsfall.newBuilder().apply {
                        doedsdato = it.doedsdato.toIsoString()
                        master = it.master ?: ""
                    })
                }

                it.telefonnummer.forEach {
                    addTelefonnummer(PersonProto.Telefonnummer.newBuilder().apply {
                        nummer = it.nummer ?: ""
                        landkode = it.landskode ?: ""
                        prioritet = it.prioritet
                    })
                }

                it.utflyttingFraNorge.forEach {
                    addUtflyttingFraNorge(PersonProto.UtflyttingFraNorge.newBuilder().apply {
                        tilflyttingsland = it.tilflyttingsland ?: ""
                        tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet ?: ""
                    })
                }

                it.talesspraaktolk.forEach {
                    addTalesspraaktolk(it ?: "")
                }

                it.fullmakt.forEach {
                    addFullmakt(PersonProto.Fullmakt.newBuilder().apply {
                        motpartsRolle = it.motpartsRolle
                        motpartsPersonident = it.motpartsPersonident
                        it.omraader.forEach { addOmraader(it) }
                        gyldigFraOgMed = it.gyldigFraOgMed.toIsoString()
                        gyldigTilOgMed = it.gyldigTilOgMed.toIsoString()
                    })
                }

                it.vergemaalEllerFremtidsfullmakt.forEach {
                    addVergemaalEllerFremtidsfullmakt(PersonProto.VergemaalEllerFremtidsfullmakt.newBuilder().apply {
                        type = it.type
                        embete = it.embete ?: ""
                        navn = PersonProto.Navn.newBuilder().apply {
                            fornavn = it.navn.fornavn ?: ""
                            mellomnavn = it.navn.mellomnavn ?: ""
                            etternavn = it.navn.etternavn ?: ""
                        }.build()
                        motpartsPersonident = it.motpartsPersonident ?: ""
                        omfang = it.omfang ?: ""
                        omfangetErInnenPersonligOmraade = it.omfangetErInnenPersonligOmraade
                    })
                }
            }.build()
        }

fun String.stringOrNull(): String? = if (this.isBlank()) null else this

fun PersonBaseFromProto(key: ByteArray, value: ByteArray?): PersonBase =
        if (value == null) { PersonBase.createPersonTombstone(key) } else {
            kotlin.runCatching {
            PersonProto.PersonValue.parseFrom(value).let { v ->
                PersonSf(
                        aktoerId = PersonProto.PersonKey.parseFrom(key).aktoerId,
                        folkeregisterId = v.folkeregisterIdList,
                        navn = v.navnList.map { Navn(
                                fornavn = it.fornavn.stringOrNull(),
                                mellomnavn = it.mellomnavn.stringOrNull(),
                                etternavn = it.etternavn.stringOrNull()
                        ) },
                        familierelasjoner = v.familierelasjonerList.map {
                            FamilieRelasjon(
                                    relatertPersonsIdent = it.relatertPersonsIdent.stringOrNull(),
                                    relatertPersonsRolle = it.relatertPersonsRolle.stringOrNull(),
                                    minRolleForPerson = it.minRolleForPerson.stringOrNull())
                        },
                        folkeregisterpersonstatus = v.folkeregisterpersonstatusList,
                        innflyttingTilNorge = v.innflyttingTilNorgeList.map {
                            InnflyttingTilNorge(
                                    fraflyttingsland = it.fraflyttingsland.stringOrNull(),
                                    fraflyttingsstedIUtlandet = it.fraflyttingsstedIUtlandet.stringOrNull()
                            )
                        },
                        adressebeskyttelse = v.adressebeskyttelseList,
                        sikkerhetstiltak = v.sikkerhetstiltakList.map {
                            Sikkerhetstiltak(
                                    beskrivelse = it.beskrivelse.stringOrNull(),
                                    tiltaksType = it.tiltakstype.stringOrNull(),
                                    gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                    gyldigTilOgMed = it.gyldigTilOgMed.toLocalDate(),
                                    kontaktpersonId = it.kontaktpersonId.stringOrNull(),
                                    kontaktpersonEnhet = it.kontaktpersonEnhet.stringOrNull()
                            )
                        },
                        bostedsadresse = Adresser(
                                vegadresse = v.bostedsadresse.vegadresseList.map {
                                    Vegadresse(
                                            kommunenummer = it.kommunenummer.stringOrNull(),
                                            adressenavn = it.adressenavn.stringOrNull(),
                                            husnummer = it.husnummer.stringOrNull(),
                                            husbokstav = it.husbokstav.stringOrNull(),
                                            postnummer = it.postnummer.stringOrNull(),
                                            bydelsnummer = it.bydelsnummer.stringOrNull(),
                                            koordinater = it.koordinater.stringOrNull()
                                    )
                                },
                                matrikkeladresse = v.bostedsadresse.matrikkeladresseList.map {
                                    Matrikkeladresse(
                                            kommunenummer = it.kommunenummer.stringOrNull(),
                                            postnummer = it.postnummer.stringOrNull(),
                                            bydelsnummer = it.bydelsnummer.stringOrNull(),
                                            koordinater = it.koordinater.stringOrNull()
                                    )
                                },
                                ukjentBosted = v.bostedsadresse.ukjentBostedList.map {
                                    UkjentBosted(bostedskommune = it.bostedskommune.stringOrNull())
                                },
                                utenlandskAdresse = v.bostedsadresse.utenlandskAdresseList.map {
                                    UtenlandskAdresse(
                                            adressenavnNummer = it.adressenavnNummer.stringOrNull(),
                                            bygningEtasjeLeilighet = it.bygningEtasjeLeilighet.stringOrNull(),
                                            postboksNummerNavn = it.postboksNummerNavn.stringOrNull(),
                                            postkode = it.postkode.stringOrNull(),
                                            bySted = it.bySted.stringOrNull(),
                                            regionDistriktOmraade = it.regionDistriktOmraade.stringOrNull(),
                                            landkode = it.landkode.stringOrNull()
                                    )
                                }
                        ),
                        oppholdsadresse = Adresser(
                                vegadresse = v.oppholdsadresse.vegadresseList.map {
                                    Vegadresse(
                                            kommunenummer = it.kommunenummer.stringOrNull(),
                                            adressenavn = it.adressenavn.stringOrNull(),
                                            husnummer = it.husnummer.stringOrNull(),
                                            husbokstav = it.husbokstav.stringOrNull(),
                                            postnummer = it.postnummer.stringOrNull(),
                                            bydelsnummer = it.bydelsnummer.stringOrNull(),
                                            koordinater = it.koordinater.stringOrNull()
                                    )
                                },
                                matrikkeladresse = v.oppholdsadresse.matrikkeladresseList.map {
                                    Matrikkeladresse(
                                            kommunenummer = it.kommunenummer.stringOrNull(),
                                            postnummer = it.postnummer.stringOrNull(),
                                            bydelsnummer = it.bydelsnummer.stringOrNull(),
                                            koordinater = it.koordinater.stringOrNull()
                                    )
                                },
                                ukjentBosted = v.oppholdsadresse.ukjentBostedList.map {
                                    UkjentBosted(bostedskommune = it.bostedskommune.stringOrNull())
                                },
                                utenlandskAdresse = v.oppholdsadresse.utenlandskAdresseList.map {
                                    UtenlandskAdresse(
                                            adressenavnNummer = it.adressenavnNummer.stringOrNull(),
                                            bygningEtasjeLeilighet = it.bygningEtasjeLeilighet.stringOrNull(),
                                            postboksNummerNavn = it.postboksNummerNavn.stringOrNull(),
                                            postkode = it.postkode.stringOrNull(),
                                            bySted = it.bySted.stringOrNull(),
                                            regionDistriktOmraade = it.regionDistriktOmraade.stringOrNull(),
                                            landkode = it.landkode.stringOrNull()
                                    )
                                }
                        ),
                        statsborgerskap = v.statsborgerskapList,
                        sivilstand = v.sivilstandList.map {
                            Sivilstand(
                                    type = it.type.stringOrNull(),
                                    gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                    relatertVedSivilstand = it.relatertVedSivilstand.stringOrNull()
                            )
                        },
                        kommunenummerFraGt = v.kommunenummerFraGt,
                        bydelsnummerFraGt = v.bydelsnummerFraGt,
                        kommunenummerFraAdresse = v.kommunenummerFraAdresse,
                        bydelsnummerFraAdresse = v.bydelsnummerFraAdresse,
                        kjoenn = v.kjoennList,
                        doedsfall = v.doedsfallList.map {
                            Doedsfall(
                                    doedsdato = it.doedsdato.toLocalDate(),
                                    master = it.master.stringOrNull()
                            )
                        },
                        telefonnummer = v.telefonnummerList.map {
                            Telefonnummer(
                                    nummer = it.nummer.stringOrNull(),
                                    landskode = it.landkode.stringOrNull(),
                                    prioritet = it.prioritet
                            )
                        },
                        utflyttingFraNorge = v.utflyttingFraNorgeList.map {
                            UtflyttingFraNorge(
                                    tilflyttingsland = it.tilflyttingsland.stringOrNull(),
                                    tilflyttingsstedIUtlandet = it.tilflyttingsstedIUtlandet.stringOrNull()
                            )
                        },
                        talesspraaktolk = v.talesspraaktolkList,
                        fullmakt = v.fullmaktList.map {
                            Fullmakt(
                                    motpartsRolle = it.motpartsRolle,
                                    motpartsPersonident = it.motpartsPersonident,
                                    omraader = it.omraaderList,
                                    gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                    gyldigTilOgMed = it.gyldigTilOgMed.toLocalDate()
                            )
                        },
                        vergemaalEllerFremtidsfullmakt = v.vergemaalEllerFremtidsfullmaktList.map {
                            VergemaalEllerFremtidsfullmakt(
                                    type = it.type,
                                    embete = it.embete.stringOrNull(),
                                    navn = Navn(fornavn = it.navn.fornavn.stringOrNull(), mellomnavn = it.navn.mellomnavn.stringOrNull(), etternavn = it.navn.etternavn.stringOrNull()),
                                    motpartsPersonident = it.motpartsPersonident.stringOrNull(),
                                    omfang = it.omfang.stringOrNull(),
                                    omfangetErInnenPersonligOmraade = it.omfangetErInnenPersonligOmraade
                            )
                        }
                )
            } }.getOrDefault(PersonProtobufIssue)
        }

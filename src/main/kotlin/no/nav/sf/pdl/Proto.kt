package no.nav.sf.pdl

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import no.nav.pdlsf.proto.PersonProto

fun LocalDate?.toIsoString(): String {
    return this?.format(DateTimeFormatter.ISO_DATE) ?: ""
}

fun Metadata?.toProto(): PersonProto.Metadata {
    return this?.let {
        PersonProto.Metadata.newBuilder().apply {
            opplysningsId = it.opplysningsId
            master = it.master
            historisk = it.historisk
            it.endringer.forEach {
                addEndringer(PersonProto.Endring.newBuilder().apply {
                    type = it.type
                    registrert = it.registrert
                    registrertAv = it.registrertAv
                    systemkilde = it.systemkilde
                    kilde = it.kilde
                })
            }
        }
                .build()
    } ?: MetadataNull.toProto()
}

fun Folkeregistermetadata?.toProto(): PersonProto.Folkeregistermetadata {
    return this?.let {
        PersonProto.Folkeregistermetadata.newBuilder().apply {
            ajourholdstidspunkt = it.ajourholdstidspunkt ?: ""
            gyldighetstidspunkt = it.gyldighetstidspunkt ?: ""
            opphoerstidspunkt = it.opphoerstidspunkt ?: ""
            kilde = it.kilde ?: ""
            aarsak = it.aarsak ?: ""
        }
                .build()
    } ?: FolkeregistermetadataNull.toProto()
}

fun PersonSf.toPersonProto(): Pair<PersonProto.PersonKey, PersonProto.PersonValue> =
        this.let {
            PersonProto.PersonKey.newBuilder().apply {
                aktoerId = it.aktoerId
            }.build() to PersonProto.PersonValue.newBuilder().apply {
                it.identer.forEach {
                    addIdenter(PersonProto.Ident.newBuilder().apply {
                        ident = it.ident
                        gruppe = it.gruppe
                        historisk = it.historisk
                        metadata = it.metadata.toProto()
                        folkeregistermetadata = it.folkeregistermetadata.toProto()
                    }.build())
                }
                it.folkeregisteridentifikator.forEach {
                    addFolkeregisteridentifikator(PersonProto.Folkeregisteridentifikator.newBuilder().apply {
                        identifikasjonsnummer = it.identifikasjonsnummer
                        type = it.type
                        status = it.status
                        metadata = it.metadata.toProto()
                        folkeregistermetadata = it.folkeregistermetadata.toProto()
                    }.build())
                }

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

                it.forelderBarnRelasjoner.forEach { fbr ->
                    addForelderBarnRelasjoner(PersonProto.ForelderBarnRelasjon.newBuilder().apply {
                        relatertPersonsIdent = fbr.relatertPersonsIdent ?: ""
                        relatertPersonsRolle = fbr.relatertPersonsRolle ?: ""
                        minRolleForPerson = fbr.minRolleForPerson ?: ""
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
                        motpartsRolle = it.motpartsRolle ?: ""
                        motpartsPersonident = it.motpartsPersonident ?: ""
                        it.omraader.forEach { addOmraader(it) }
                        gyldigFraOgMed = it.gyldigFraOgMed.toIsoString()
                        gyldigTilOgMed = it.gyldigTilOgMed.toIsoString()
                    })
                }

                it.vergemaalEllerFremtidsfullmakt.forEach {
                    addVergemaalEllerFremtidsfullmakt(PersonProto.VergemaalEllerFremtidsfullmakt.newBuilder().apply {
                        type = it.type ?: ""
                        embete = it.embete ?: ""
                        navn = PersonProto.Navn.newBuilder().apply {
                            fornavn = it.navn?.fornavn ?: ""
                            mellomnavn = it.navn?.mellomnavn ?: ""
                            etternavn = it.navn?.etternavn ?: ""
                        }.build()
                        motpartsPersonident = it.motpartsPersonident ?: ""
                        omfang = it.omfang ?: ""
                        omfangetErInnenPersonligOmraade = it.omfangetErInnenPersonligOmraade.asProtoString()
                    })
                }

                it.foedselsdato.forEach {
                    addFoedselsdato(it)
                }
            }.build()
        }

fun Boolean?.asProtoString(): String =
        if (this == null) {
            ""
        } else {
            if (this) {
                "true"
            } else {
                "false"
            }
        }

fun String.booleanOrNull(): Boolean? =
        if (this == "true") {
            true
        } else {
            if (this == "false") {
                false
            } else {
                null
            }
        }

fun String.stringOrNull(): String? = if (this.isBlank()) null else this

fun metaDataFromProto(proto: PersonProto.Metadata): Metadata? {
    val metaData = Metadata(proto.opplysningsId, proto.master, proto.historisk, proto.endringerList.map {
        Endring(it.type, it.registrert, it.registrertAv, it.systemkilde, it.kilde)
    })
    return if (metaData.opplysningsId == MetadataNull.opplysningsId) null else metaData
}

fun folkeregistermetadataFromProto(proto: PersonProto.Folkeregistermetadata): Folkeregistermetadata? {
    val folkeregistermetadata = Folkeregistermetadata(proto.ajourholdstidspunkt.stringOrNull(),
            proto.gyldighetstidspunkt.stringOrNull(), proto.opphoerstidspunkt.stringOrNull(), proto.kilde.stringOrNull(), proto.aarsak.stringOrNull())
    return if (folkeregistermetadata.ajourholdstidspunkt == FolkeregistermetadataNull.ajourholdstidspunkt) null else folkeregistermetadata
}

fun ByteArray.toPersonSf(aktoerId : String) : PersonBase {
    return kotlin.runCatching {
        PersonProto.PersonValue.parseFrom(this).let { v ->
            PersonSf(
                    identer = v.identerList.map {
                        Ident(
                                ident = it.ident,
                                historisk = it.historisk,
                                gruppe = it.gruppe,
                                metadata = metaDataFromProto(it.metadata),
                                folkeregistermetadata = folkeregistermetadataFromProto(it.folkeregistermetadata))
                    },
                    folkeregisteridentifikator = v.folkeregisteridentifikatorList.map {
                        Folkeregisteridentifikator(
                                identifikasjonsnummer = it.identifikasjonsnummer,
                                type = it.type,
                                status = it.status,
                                metadata = metaDataFromProto(it.metadata),
                                folkeregistermetadata = folkeregistermetadataFromProto(it.folkeregistermetadata))
                    },
                    aktoerId = aktoerId,
                    folkeregisterId = v.folkeregisterIdList,
                    navn = v.navnList.map {
                        Navn(
                                fornavn = it.fornavn.stringOrNull(),
                                mellomnavn = it.mellomnavn.stringOrNull(),
                                etternavn = it.etternavn.stringOrNull()
                        )
                    },
                    forelderBarnRelasjoner = v.forelderBarnRelasjonerList.map {
                        ForelderBarnRelasjon(
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
                                motpartsRolle = it.motpartsRolle.stringOrNull(),
                                motpartsPersonident = it.motpartsPersonident.stringOrNull(),
                                omraader = it.omraaderList,
                                gyldigFraOgMed = it.gyldigFraOgMed.toLocalDate(),
                                gyldigTilOgMed = it.gyldigTilOgMed.toLocalDate()
                        )
                    },
                    vergemaalEllerFremtidsfullmakt = v.vergemaalEllerFremtidsfullmaktList.map {
                        VergemaalEllerFremtidsfullmakt(
                                type = it.type.stringOrNull(),
                                embete = it.embete.stringOrNull(),
                                navn = if (it.navn.fornavn == "" && it.navn.mellomnavn == "" && it.navn.etternavn == "") null else Navn(fornavn = it.navn.fornavn.stringOrNull(), mellomnavn = it.navn.mellomnavn.stringOrNull(), etternavn = it.navn.etternavn.stringOrNull()),
                                motpartsPersonident = it.motpartsPersonident.stringOrNull(),
                                omfang = it.omfang.stringOrNull(),
                                omfangetErInnenPersonligOmraade = it.omfangetErInnenPersonligOmraade.booleanOrNull()
                        )
                    },
                    foedselsdato = v.foedselsdatoList
            )
        }
    }.getOrDefault(PersonProtobufIssue)
}

fun PersonBaseFromProto(key: ByteArray, value: ByteArray?): PersonBase =
        if (value == null) {
            PersonBase.createPersonTombstone(key)
        } else {
            kotlin.runCatching {
                value.toPersonSf(PersonProto.PersonKey.parseFrom(key).aktoerId)
            }.getOrDefault(PersonProtobufIssue)
        }

fun GtValue.toGtProto(): Pair<PersonProto.PersonKey, PersonProto.Gt> =
        this.let {
            PersonProto.PersonKey.newBuilder().apply {
                aktoerId = it.aktoerId
            }.build() to PersonProto.Gt.newBuilder().apply {
                kommunenummerFraGt = it.kommunenummerFraGt
                bydelsnummerFraGt = it.bydelsnummerFraGt
            }.build()
        }

fun GtBaseFromProto(key: ByteArray, value: ByteArray?): GtValueBase =
        if (value == null) {
            GtValueBase.createGtTombstone(key)
        } else {
            runCatching {
                PersonProto.Gt.parseFrom(value).let { gt ->
                    GtValue(
                            aktoerId = PersonProto.PersonKey.parseFrom(key).aktoerId,
                            kommunenummerFraGt = gt.kommunenummerFraGt,
                            bydelsnummerFraGt = gt.bydelsnummerFraGt
                    )
                }
            }.getOrDefault(GtProtobufIssue)
        }

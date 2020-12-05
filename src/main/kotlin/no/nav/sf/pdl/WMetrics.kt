package no.nav.sf.pdl

import io.prometheus.client.Gauge
import java.time.LocalDate
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun registerGauge(name: String): Gauge {
    return Gauge.build().name(name).help(name).register()
}

fun registerLabelGauge(name: String, label: String): Gauge {
    return Gauge.build().name(name).help(name).labelNames(label).register()
}

data class WMetrics(
    val oldSyntaxTest: Gauge = Gauge
            .build()
            .name("old_syntax_test")
            .help("Something")
            .register(),
    val published_by_gt_update: Gauge = registerGauge("published_by_gt_update"),
    val enriching_from_gt_cache: Gauge = registerGauge("enriching_from_gt_cache"),

    val gt_cache_blocked: Gauge = registerGauge("gt_cache_blocked"),
    val gt_cache_blocked_tombstone: Gauge = registerGauge("gt_cache_blocked_tombstone"),
    val gt_cache_new: Gauge = registerGauge("gt_cache_new"),
    val gt_cache_new_tombstone: Gauge = registerGauge("gt_cache_new_tombstone"),
    val gt_cache_update: Gauge = registerGauge("gt_cache_update"),
    val gt_cache_update_tombstone: Gauge = registerGauge("gt_cache_update_tombstone"),
    val gt_cache_size_total: Gauge = registerGauge("gt_cache_size_total"),
    val gt_cache_size_tombstones: Gauge = registerGauge("gt_cache_size_tombstone"),

    val cache_blocked: Gauge = registerGauge("cache_blocked"),
    val cache_blocked_tombstone: Gauge = registerGauge("cache_blocked_tombstone"),
    val cache_new: Gauge = registerGauge("cache_new"),
    val cache_new_tombstone: Gauge = registerGauge("cache_new_tombstone"),
    val cache_update: Gauge = registerGauge("cache_update"),
    val cache_update_tombstone: Gauge = registerGauge("cache_update_tombstone"),
    val cache_size_total: Gauge = registerGauge("cache_size_total"),
    val cache_size_tombstones: Gauge = registerGauge("cache_size_tombstone"),

    val gtPublished: Gauge = registerGauge("gt_published"),
    val gtPublishedTombstone: Gauge = registerGauge("gt_published_tombstone"),

    val gtRecordsParsed: Gauge = registerGauge("gt_records_parsed"),

    val cacheRecordsParsed: Gauge = registerGauge("cache_records_parsed"),
    val gtCacheRecordsParsed: Gauge = registerGauge("gt_cache_records_parsed"),

    val testRunRecordsParsed: Gauge = registerGauge("test_run_records_parsed"), // Undistinct at test run

    val initialRecordsParsed: Gauge = registerGauge("initial_records_parsed"), // Undistinct at init
    val initialPersons: Gauge = registerGauge("initial_persons"), // Undistinct at init
    val initialTombstones: Gauge = registerGauge("initial_tombstones"), // Undistinct at init
    val initialPublishedPersons: Gauge = registerGauge("initial_published_persons"),
    val initialPublishedTombstones: Gauge = registerGauge("initial_published_tombstones"),

    val deadPersons: Gauge = registerGauge("dead_persons"), // distinct at init and work
    val deadPersonsWithoutDate: Gauge = registerGauge("dead_persons_without_date"), // distinct at init and work
    val livingPersons: Gauge = registerGauge("living_persons"), // distinct at init and work
    val tombstones: Gauge = registerGauge("tombstones"), // distinct at init and work

    val recordsParsed: Gauge = registerGauge("records_parsed"), // Undistinct at work

    val publishedPersons: Gauge = registerGauge("published_persons"),
    val publishedTombstones: Gauge = registerGauge("published_tombstones"),

    val consumerIssues: Gauge = registerGauge("consumer_issues"),
    val producerIssues: Gauge = registerGauge("producer_issues"),

    val kommunenummerMissing: Gauge = registerGauge("kommunenummer_missing"),
    val kommunenummerOnlyFromAdresse: Gauge = registerGauge("kommunenummer_only_from_adresse"),
    val kommunenummerOnlyFromGt: Gauge = registerGauge("kommunenummer_only_from_gt"),
    val kommunenummerFromBothAdresseAndGt: Gauge = registerGauge("kommunenummer_from_both_adresse_and_gt"),
    val kommunenummerFromAdresseAndGtIsTheSame: Gauge = registerGauge("kommunenummer_from_adresse_and_gt_is_the_same"),
    val kommunenummerFromAdresseAndGtDiffer: Gauge = registerGauge("kommunenummer_from_adresse_and_gt_differ"),

    val bydelsnummerMissing: Gauge = registerGauge("bydelsnummer_missing"),
    val bydelsnummerOnlyFromAdresse: Gauge = registerGauge("bydelsnummer_only_from_adresse"),
    val bydelsnummerOnlyFromGt: Gauge = registerGauge("bydelsnummer_only_from_gt"),
    val bydelsnummerFromBothAdresseAndGt: Gauge = registerGauge("bydelsnummer_from_both_adresse_and_gt"),
    val bydelsnummerFromAdresseAndGtIsTheSame: Gauge = registerGauge("bydelsnummer_from_adresse_and_gt_is_the_same"),
    val bydelsnummerFromAdresseAndGtDiffer: Gauge = registerGauge("bydelsnummer_from_adresse_and_gt_differ"),

    val kommune: Gauge = registerLabelGauge("kommune", "kommune"),
    val kommune_number_not_found: Gauge = registerLabelGauge("kommune_number_not_found", "kommune_number")
) {
    fun clearAll() {
        oldSyntaxTest.clear()
        oldSyntaxTest.set(10.0)
        published_by_gt_update.clear()
        enriching_from_gt_cache.clear()

        cache_blocked.clear()
        cache_blocked_tombstone.clear()
        cache_new.clear()
        cache_new_tombstone.clear()
        cache_update.clear()
        cache_update_tombstone.clear()
        cache_size_total.clear()
        cache_size_tombstones.clear()

        gt_cache_blocked.clear()
        gt_cache_blocked_tombstone.clear()
        gt_cache_new.clear()
        gt_cache_new_tombstone.clear()
        gt_cache_update.clear()
        gt_cache_update_tombstone.clear()
        gt_cache_size_total.clear()
        gt_cache_size_tombstones.clear()
        gtCacheRecordsParsed.clear()
        gtPublished.clear()
        gtPublishedTombstone.clear()
        gtRecordsParsed.clear()
        cacheRecordsParsed.clear()

        testRunRecordsParsed.clear()

        initialRecordsParsed.clear()
        initialPersons.clear()
        initialTombstones.clear()
        initialPublishedPersons.clear()
        initialPublishedTombstones.clear()

        deadPersons.clear()
        deadPersonsWithoutDate.clear()
        livingPersons.clear()
        tombstones.clear()

        recordsParsed.clear()

        publishedPersons.clear()
        publishedTombstones.clear()

        consumerIssues.clear()
        producerIssues.clear()

        kommunenummerMissing.clear()
        kommunenummerOnlyFromAdresse.clear()
        kommunenummerOnlyFromGt.clear()
        kommunenummerFromBothAdresseAndGt.clear()
        kommunenummerFromAdresseAndGtIsTheSame.clear()
        kommunenummerFromAdresseAndGtDiffer.clear()

        bydelsnummerMissing.clear()
        bydelsnummerOnlyFromAdresse.clear()
        bydelsnummerOnlyFromGt.clear()
        bydelsnummerFromBothAdresseAndGt.clear()
        bydelsnummerFromAdresseAndGtIsTheSame.clear()
        bydelsnummerFromAdresseAndGtDiffer.clear()

        kommune.clear()
        kommune_number_not_found.clear()
    }

    fun measureKommune(kommunenummer: String) {
        val kommuneLabel = if (kommunenummer == UKJENT_FRA_PDL) {
            UKJENT_FRA_PDL
        } else {
            PostnummerService.getKommunenummer(kommunenummer)?.let {
                it
            } ?: workMetrics.kommune_number_not_found.labels(kommunenummer).inc().let { NOT_FOUND_IN_REGISTER }
        }
        workMetrics.kommune.labels(kommuneLabel).inc()
    }

    val investigateList: MutableList<PersonSf> = mutableListOf()
    fun measureNummerSources(person: PersonSf, investigate: Boolean = false) {
        when {
            person.kommunenummerFraGt == UKJENT_FRA_PDL && person.kommunenummerFraAdresse == UKJENT_FRA_PDL -> {
                workMetrics.kommunenummerMissing.inc()
            }
            person.kommunenummerFraGt != UKJENT_FRA_PDL && person.kommunenummerFraAdresse == UKJENT_FRA_PDL -> {
                workMetrics.kommunenummerOnlyFromGt.inc()
            }
            person.kommunenummerFraGt == UKJENT_FRA_PDL && person.kommunenummerFraAdresse != UKJENT_FRA_PDL -> {
                workMetrics.kommunenummerOnlyFromAdresse.inc()
            }
            person.kommunenummerFraGt != UKJENT_FRA_PDL && person.kommunenummerFraAdresse != UKJENT_FRA_PDL -> {
                workMetrics.kommunenummerFromBothAdresseAndGt.inc()
                if (person.kommunenummerFraGt == person.kommunenummerFraAdresse) {
                    workMetrics.kommunenummerFromAdresseAndGtIsTheSame.inc()
                } else {
                    if (investigate) log.info { "Found case with kommune fra adresse and gt differ, total: ${workMetrics.kommunenummerFromAdresseAndGtIsTheSame}" }
                    if (investigate && investigateList.size < 10) investigateList.add(person)
                    workMetrics.kommunenummerFromAdresseAndGtDiffer.inc()
                }
            }
        }
        when {
            person.bydelsnummerFraGt == UKJENT_FRA_PDL && person.bydelsnummerFraAdresse == UKJENT_FRA_PDL -> {
                workMetrics.bydelsnummerMissing.inc()
            }
            person.bydelsnummerFraGt != UKJENT_FRA_PDL && person.bydelsnummerFraAdresse == UKJENT_FRA_PDL -> {
                workMetrics.bydelsnummerOnlyFromGt.inc()
            }
            person.bydelsnummerFraGt == UKJENT_FRA_PDL && person.bydelsnummerFraAdresse != UKJENT_FRA_PDL -> {
                workMetrics.bydelsnummerOnlyFromAdresse.inc()
            }
            person.bydelsnummerFraGt != UKJENT_FRA_PDL && person.bydelsnummerFraAdresse != UKJENT_FRA_PDL -> {
                workMetrics.bydelsnummerFromBothAdresseAndGt.inc()
                if (person.bydelsnummerFraGt == person.bydelsnummerFraAdresse) {
                    workMetrics.bydelsnummerFromAdresseAndGtIsTheSame.inc()
                } else {
                    if (investigate) investigateList.add(person)
                    workMetrics.bydelsnummerFromAdresseAndGtDiffer.inc()
                }
            }
        }
    }

    var earliestDeath: LocalDate? = null
    fun measureLivingOrDead(person: PersonSf) {
        if (person.isDead()) {
            workMetrics.deadPersons.inc()
            if (person.doedsfall.any { it.doedsdato != null }) {
                person.doedsfall.filter { it.doedsdato != null }.forEach { doedsfall ->
                    doedsfall.doedsdato?.let {
                        earliestDeath = if (earliestDeath == null) {
                            it
                        } else {
                            if (it.isBefore(earliestDeath)) it else earliestDeath
                        }
                    }
                }
            } else {
                workMetrics.deadPersonsWithoutDate.inc()
            }
        } else {
            workMetrics.livingPersons.inc()
        }
    }

    fun measurePersonStats(person: PersonSf, investigate: Boolean = false) {
        workMetrics.measureLivingOrDead(person)
        workMetrics.measureNummerSources(person, investigate)
        if (person.kommunenummerFraGt != UKJENT_FRA_PDL) {
            workMetrics.measureKommune(person.kommunenummerFraGt)
        } else {
            workMetrics.measureKommune(person.kommunenummerFraAdresse)
        }
    }
}

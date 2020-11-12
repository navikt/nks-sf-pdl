package no.nav.sf.pdl

import io.prometheus.client.Gauge

fun registerGauge(name: String): Gauge {
    return Gauge.build().name(name).help(name).register()
}

fun registerLabelGauge(name: String, label: String): Gauge {
    return Gauge.build().name(name).help(name).labelNames(label).register()
}

data class WMetrics(
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
        testRunRecordsParsed.clear()

        initialRecordsParsed.clear()
        initialPersons.clear()
        initialTombstones.clear()
        initialPublishedPersons.clear()
        initialPublishedTombstones.clear()

        deadPersons.clear()
        livingPersons.clear()
        tombstones.clear()

        publishedPersons.clear()
        publishedTombstones.clear()

        consumerIssues.clear()
        producerIssues.clear()

        kommunenummerMissing.clear()
        kommunenummerOnlyFromAdresse.clear()
        kommunenummerOnlyFromGt.clear()
        kommunenummerFromAdresseAndGtIsTheSame.clear()
        kommunenummerFromAdresseAndGtDiffer.clear()

        bydelsnummerMissing.clear()
        bydelsnummerOnlyFromAdresse.clear()
        bydelsnummerOnlyFromGt.clear()
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
}

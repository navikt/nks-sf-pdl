package no.nav.sf.pdl

import java.io.FileOutputStream
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

interface Investigate {
    companion object {
        fun writeText(text: String, append: Boolean = false, filename: String = "/tmp/investigate") {
            val timeStamp = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneOffset.systemDefault())
                    .format(Instant.now())
            // if (filename != "/tmp/investigate") {
                FileOutputStream(filename, append).bufferedWriter().use { writer ->
                    writer.write("$timeStamp : $text \n")
                } // TODO Muted Investigate for later use
            // }
        }
    }
}

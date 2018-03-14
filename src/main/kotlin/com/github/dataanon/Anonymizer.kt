package com.github.dataanon

import com.github.dataanon.dsl.Whitelist
import com.github.dataanon.model.DbConfig
import com.github.dataanon.model.Field
import com.github.dataanon.model.Record
import com.github.dataanon.strategy.AnonymizationStrategy
import com.github.dataanon.strategy.datetime.DateRandomDelta
import com.github.dataanon.strategy.datetime.DateTimeRandomDelta
import com.github.dataanon.strategy.list.PickFromDatabase
import com.github.dataanon.strategy.number.FixedDouble
import java.time.Duration

fun main(args: Array<String>) {

    // Download and start H2 database server
    // Connect to source (movies_source) database and execute create_tables.sql script
    // Connect to destination (movies_dest) database and execute create_tables.sql script
    // Insert sample data into source database tables using scripts insert_movies.sql and insert_ratings.sql
    // Run this main program

    val source = DbConfig("jdbc:h2:tcp://localhost/~/movies_source", "sa", "")
    val dest = DbConfig("jdbc:h2:tcp://localhost/~/movies_dest", "sa", "")

    Whitelist(source,dest)
        .table("MOVIES") {
            where("GENRE = 'Drama'")
            limit(1_00_000)
            whitelist("MOVIE_ID")
            anonymize("TITLE").using(object: AnonymizationStrategy<String>{
                override fun anonymize(field: Field<String>, record: Record): String = "MY MOVIE ${record.rowNum}"
            })
            anonymize("GENRE").using(PickFromDatabase<String>(source,"SELECT DISTINCT GENRE FROM MOVIES"))
            anonymize("RELEASE_DATE").using(DateRandomDelta(10))
        }
        .table("RATINGS") {
            whitelist("MOVIE_ID","USER_ID")
            anonymize("RATING").using(FixedDouble(4.3))
            anonymize("CREATED_AT").using(DateTimeRandomDelta(Duration.ofSeconds(2000)))
        }
        .execute(true)
}


